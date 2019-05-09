#include <netdb.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <sys/eventfd.h>
#include <sys/epoll.h>
#include <rdma/rdma_cma.h>
#include <map>
#include <vector>
#include <iostream>
#include "get_clock.h"

#define TEST_NZ(x) do { if ( (x)) die("error: " #x " failed (returned non-zero)." ); } while (0)
#define TEST_Z(x)  do { if (!(x)) die("error: " #x " failed (returned zero/null)."); } while (0)
using namespace std;

const int BUFFER_SIZE = 1000;
const int TIMEOUT_IN_MS = 500; /* ms */

struct context {
  struct ibv_context *ctx;
  struct ibv_pd *pd;
  struct ibv_cq *cq;
  struct ibv_comp_channel *comp_channel;

  pthread_t cq_poller_thread;
};

struct connection {
  struct rdma_cm_id *id;
  struct ibv_qp *qp;

  struct ibv_mr *recv_mr;
  struct ibv_mr *send_mr;

  char *recv_region;
  char *send_region;

  int num_completions;
};

struct para
{
  int efd;
  char *filename;
//  struct ibv_send_wr wr;
};

struct para_worker
{
  int num;
  int *efd;
//  map <int, struct ibv_send_wr> m;
};

struct para_wr
{
  uint64_t addr;
  uint32_t length;
  uint32_t lkey;
};

static void die(const char *reason);

static void build_context(struct ibv_context *verbs);
static void build_qp_attr(struct ibv_qp_init_attr *qp_attr);
static void * poll_cq(void *);
static void post_receives(struct connection *conn);
static void register_memory(struct connection *conn);

static int on_addr_resolved(struct rdma_cm_id *id);
static void on_completion(struct ibv_wc *wc);
void * on_connection(void *arg);
void * worker(void *arg);
static int on_disconnect(struct rdma_cm_id *id);
static int on_event(struct rdma_cm_event *event);
static int on_route_resolved(struct rdma_cm_id *id);

static struct context *s_ctx = NULL;
struct rdma_cm_id *conn_id= NULL;
struct connection *conn_ctx = NULL;

map <int, vector<struct para_wr>> m;

int iters = 1000;
pthread_mutex_t mutex = PTHREAD_MUTEX_INITIALIZER;


int main(int argc, char **argv)
{
  struct addrinfo *addr;
  struct rdma_cm_event *event = NULL;
//  struct rdma_cm_id *conn= NULL;
  struct rdma_event_channel *ec = NULL;
  char *port = "22222";
  int thread_num = stoi(argv[2]);
  if (argc != 3 || thread_num < 1)
    die("usage: client <server-address> <thread-num>, thread-num > 0");

  TEST_NZ(getaddrinfo(argv[1], port, NULL, &addr));

  TEST_Z(ec = rdma_create_event_channel());
  TEST_NZ(rdma_create_id(ec, &conn_id, NULL, RDMA_PS_TCP));
  TEST_NZ(rdma_resolve_addr(conn_id, NULL, addr->ai_addr, TIMEOUT_IN_MS));

  freeaddrinfo(addr);

  while (rdma_get_cm_event(ec, &event) == 0) {
    struct rdma_cm_event event_copy;

    memcpy(&event_copy, event, sizeof(*event));
    rdma_ack_cm_event(event);

    if (on_event(&event_copy))
      break;
  }

  conn_ctx = (struct connection *)(conn_id->context);

  pthread_t worker_t;
  pthread_t *t = new pthread_t[thread_num];
  int *efd = new int[thread_num];
//  struct ibv_send_wr *wr = new struct ibv_send_wr[thread_num];
  char **filename = new char*[thread_num];
  struct para *parameters = new struct para[thread_num];
  struct para_worker parameters_worker;
//  map <int, struct ibv_send_wr> m;
  void *status;

  for(int i = 0; i < thread_num; i++)
  {
    filename[i] = new char[20];
    snprintf(filename[i], 20, "./out/out%d.list", i + 1);
    efd[i] = eventfd(0, 0);
    parameters[i].filename = filename[i];
    parameters[i].efd = efd[i];
//    pthread_create(&t[i], NULL, on_connection, &(parameters[i]));
  }
  parameters_worker.num = thread_num;
  parameters_worker.efd = efd;
//  parameters_worker.m = m;

  pthread_create(&worker_t, NULL, worker, &parameters_worker);

  for(int i = 0; i < thread_num; i++)
  {
    pthread_create(&t[i], NULL, on_connection, &(parameters[i]));
  }

  for(int i = 0; i < thread_num; i++)
    pthread_join(t[i], &status);

//  pthread_join(s_ctx->cq_poller_thread, &status);
  pthread_join(worker_t, &status);

  delete []t;
  for(int i = 0; i < thread_num; i++)
    delete []filename[i];

  delete []filename;
  delete []efd;
  delete []parameters;
  rdma_destroy_event_channel(ec);

  return 0;
}

void die(const char *reason)
{
  fprintf(stderr, "%s\n", reason);
  exit(EXIT_FAILURE);
}

void build_context(struct ibv_context *verbs)
{
  if (s_ctx) {
    if (s_ctx->ctx != verbs)
      die("cannot handle events in more than one context.");

    return;
  }

  s_ctx = (struct context *)malloc(sizeof(struct context));

  s_ctx->ctx = verbs;

  TEST_Z(s_ctx->pd = ibv_alloc_pd(s_ctx->ctx));
  TEST_Z(s_ctx->comp_channel = ibv_create_comp_channel(s_ctx->ctx));
  TEST_Z(s_ctx->cq = ibv_create_cq(s_ctx->ctx, 100, NULL, s_ctx->comp_channel, 0)); /* cqe=10 is arbitrary */
  TEST_NZ(ibv_req_notify_cq(s_ctx->cq, 0));

//  TEST_NZ(pthread_create(&s_ctx->cq_poller_thread, NULL, poll_cq, NULL));
}

void build_qp_attr(struct ibv_qp_init_attr *qp_attr)
{
  memset(qp_attr, 0, sizeof(*qp_attr));

  qp_attr->send_cq = s_ctx->cq;
  qp_attr->recv_cq = s_ctx->cq;
  qp_attr->qp_type = IBV_QPT_RC;

  qp_attr->cap.max_send_wr = 1000;
  qp_attr->cap.max_recv_wr = 1000;
  qp_attr->cap.max_send_sge = 1;
  qp_attr->cap.max_recv_sge = 1;
}

void * poll_cq(void *ctx)
{
  struct ibv_cq *cq;
  struct ibv_wc wc;
  while (1) {
 
    TEST_NZ(ibv_get_cq_event(s_ctx->comp_channel, &cq, &ctx));
    ibv_ack_cq_events(cq, 1);
    TEST_NZ(ibv_req_notify_cq(cq, 0));

    while (ibv_poll_cq(cq, 1, &wc))
    {
      on_completion(&wc);
    }
  }

  return NULL;
}

void post_receives(struct connection *conn)
{
  struct ibv_recv_wr wr, *bad_wr = NULL;
  struct ibv_sge sge;

  wr.wr_id = (uintptr_t)conn;
  wr.next = NULL;
  wr.sg_list = &sge;
  wr.num_sge = 1;

  sge.addr = (uintptr_t)conn->recv_region;
  sge.length = BUFFER_SIZE;
  sge.lkey = conn->recv_mr->lkey;

  TEST_NZ(ibv_post_recv(conn->qp, &wr, &bad_wr));
}

void register_memory(struct connection *conn)
{
  conn->send_region = (char *)malloc(BUFFER_SIZE);
  conn->recv_region = (char *)malloc(BUFFER_SIZE);
  snprintf(conn->send_region, BUFFER_SIZE, "message from active/client side with pid %d", getpid());

  TEST_Z(conn->send_mr = ibv_reg_mr(
    s_ctx->pd, 
    conn->send_region, 
    BUFFER_SIZE, 
    IBV_ACCESS_LOCAL_WRITE | IBV_ACCESS_REMOTE_WRITE));

  TEST_Z(conn->recv_mr = ibv_reg_mr(
    s_ctx->pd, 
    conn->recv_region, 
    BUFFER_SIZE, 
    IBV_ACCESS_LOCAL_WRITE | IBV_ACCESS_REMOTE_WRITE));
}

int on_addr_resolved(struct rdma_cm_id *id)
{
  struct ibv_qp_init_attr qp_attr;
  struct connection *conn;

  printf("address resolved.\n");

  build_context(id->verbs);
  build_qp_attr(&qp_attr);
 
  TEST_NZ(rdma_create_qp(id, s_ctx->pd, &qp_attr));

  id->context = conn = (struct connection *)malloc(sizeof(struct connection));
 
  conn->id = id;
  conn->qp = id->qp;
  conn->num_completions = 0;

  register_memory(conn);
  post_receives(conn);
  
  TEST_NZ(rdma_resolve_route(id, TIMEOUT_IN_MS));

  return 0;
}

void on_completion(struct ibv_wc *wc)
{
  struct connection *conn = (struct connection *)(uintptr_t)wc->wr_id;

  if (wc->status != IBV_WC_SUCCESS)
  {
    cout << "status: " << wc->status << endl;
    die("on_completion: status is not IBV_WC_SUCCESS.");
  }

  if (wc->opcode & IBV_WC_RECV)
    printf("received message: %s\n", conn->recv_region);
  else if (wc->opcode == IBV_WC_SEND);
//    printf("send completed successfully.\n");
  else
    die("on_completion: completion isn't a send or a receive.");

//  if (++conn->num_completions == 2)
//    rdma_disconnect(conn->id);
}

void print_report(int iter, cycles_t *tposted, cycles_t *tcompleted, char *name)
{
  int i;
  double cpu_mhz = get_cpu_mhz(1);
  double lat_us;
  FILE *f = fopen(name, "w");
  fprintf(f, "iter\t\tLatency\n");
  for(i = 0; i < iter; i++)
  {
    lat_us = (double)(tcompleted[i] - tposted[i]) / cpu_mhz;
    fprintf(f, "%ld\t\t%.2f\n", i + 1, lat_us);
  }
  double total_lat = (double)(tcompleted[iter-1] - tposted[0]) / cpu_mhz;
  fprintf(f, "\n%.2f\n", total_lat);

  fclose(f);
}


void * on_connection(void *arg)
{
//  struct connection *conn = (struct connection *)(conn_id->context);
  struct ibv_send_wr wr, *bad_wr = NULL;
  struct ibv_sge sge;
  struct ibv_wc wc;
  struct para_wr p_wr;
//  snprintf(conn_ctx->send_region, BUFFER_SIZE, "message from active/client side with pid %d", getpid());

  printf("connected. posting send...\n");

  memset(&wr, 0, sizeof(wr));

  wr.wr_id = (uintptr_t)conn_ctx;
  wr.opcode = IBV_WR_SEND;
  wr.sg_list = &sge;
  wr.num_sge = 1;
  wr.send_flags = IBV_SEND_SIGNALED;

  p_wr.addr = (uintptr_t)conn_ctx->send_region;
  p_wr.length = BUFFER_SIZE;
  p_wr.lkey = conn_ctx->send_mr->lkey;
//  cout << "lkey: " << sge.lkey << endl;
//  int ne = 0
/*  cycles_t *c1, *c2;
  c1 = (cycles_t *)malloc(iters * sizeof *c1);
  pthread_t tid = pthread_self();

  if (!c1)
  {
    perror("malloc");
  }

  c2 = (cycles_t *)malloc(iters * sizeof *c2);

  if (!c2)
  {
    perror("malloc");
  }*/
  
  struct para *pa = (struct para *)arg;
  
//  pthread_mutex_lock(&mutex);

 
  for(int i = 0; i < iters; i++)
  {
//    c1[i] = get_cycles();
//    TEST_NZ(ibv_post_send(conn_ctx->qp, &wr, &bad_wr));
    m[pa->efd].push_back(p_wr);
//    do {
//       ne = ibv_poll_cq(s_ctx->cq, 1, &wc);
//    } while (ne == 0);
//    c2[i] = get_cycles();
//    on_completion(&wc);
//    cout << endl << (unsigned int)tid << endl;
  }
  

//  pthread_mutex_unlock(&mutex);
  uint64_t count = 1;
  int ret = write(pa->efd, &count, sizeof(count));
  if (ret < 0)
    perror("write event fd fail:");


//  print_report(iters, c1, c2, pa->filename);

}

void * worker(void *arg)
{
  struct para_worker *pa_worker = (struct para_worker *)arg;
//  struct ibv_send_wr *bad_wr = NULL;
  uint64_t number = 0;
  int epfd = epoll_create(1000);
  struct epoll_event ev;
  struct epoll_event events[1000];
  ev.events = EPOLLIN;
  for(int i = 0; i < pa_worker->num; i++)
  {
    ev.data.fd = pa_worker->efd[i];
    epoll_ctl(epfd, EPOLL_CTL_ADD, pa_worker->efd[i], &ev);
  }

  struct ibv_wc wc;
  int ne = 0, start = 0;
  cycles_t *c1, *c2;
  c1 = (cycles_t *)malloc(iters * pa_worker->num * sizeof *c1);
  pthread_t tid = pthread_self();

  if (!c1)
  {
    perror("malloc");
  }

  c2 = (cycles_t *)malloc(iters * pa_worker->num * sizeof *c2);

  if (!c2)
  {
    perror("malloc");
  }

  struct ibv_send_wr wr, *bad_wr = NULL;
  memset(&wr, 0, sizeof(wr));
  struct ibv_sge sge;

  memset(&wr, 0, sizeof(wr));

  wr.wr_id = (uintptr_t)conn_ctx;
  wr.opcode = IBV_WR_SEND;
  wr.sg_list = &sge;
  wr.num_sge = 1;
  wr.send_flags = IBV_SEND_SIGNALED;

  while(1)
  {
    int count = epoll_wait(epfd, events, 10, 2000);
//    cout << "count:" << count <<endl;
    for(int i = 0; i < count; i++)
    {
        read(events[i].data.fd, &number, sizeof(number));
        
        for(int j = 0; j < iters; j++)
        {
          sge.addr = m[events[i].data.fd][j].addr;
          sge.length = m[events[i].data.fd][j].length;
          sge.lkey = m[events[i].data.fd][j].lkey;

          c1[start + j] = get_cycles();
          TEST_NZ(ibv_post_send(conn_ctx->qp, &wr, &bad_wr));
          do {
            ne = ibv_poll_cq(s_ctx->cq, 1, &wc);
          } while (ne == 0);
          c2[start + j] = get_cycles();
          on_completion(&wc);
        }
        
        start = start + iters;
//        cout << "start: " << start << endl;
    }

    if(start >= iters * pa_worker->num)
    {
      print_report(iters * pa_worker->num, c1, c2, "out.list");
      break;
    }
  }
}


int on_disconnect(struct rdma_cm_id *id)
{
  struct connection *conn = (struct connection *)id->context;

  printf("disconnected.\n");

  rdma_destroy_qp(id);

  ibv_dereg_mr(conn->send_mr);
  ibv_dereg_mr(conn->recv_mr);

  free(conn->send_region);
  free(conn->recv_region);

  free(conn);

  rdma_destroy_id(id);

  return 1; /* exit event loop */
}

int on_event(struct rdma_cm_event *event)
{
  int r = 0;

  if (event->event == RDMA_CM_EVENT_ADDR_RESOLVED)
    r = on_addr_resolved(event->id);
  else if (event->event == RDMA_CM_EVENT_ROUTE_RESOLVED)
    r = on_route_resolved(event->id);
  else if (event->event == RDMA_CM_EVENT_ESTABLISHED)
    r = 1;
//    r = on_connection(event->id->context);
  else if (event->event == RDMA_CM_EVENT_DISCONNECTED)
    r = on_disconnect(event->id);
  else
    die("on_event: unknown event.");

  return r;
}

int on_route_resolved(struct rdma_cm_id *id)
{
  struct rdma_conn_param cm_params;

  printf("route resolved.\n");

  memset(&cm_params, 0, sizeof(cm_params));
  TEST_NZ(rdma_connect(id, &cm_params));
  
  return 0;
}

/*
*
*  MIT License
*
*  Copyright (c) 2017 finixbit <finix@protonmail.com>
*
*  Permission is hereby granted, free of charge, to any person obtaining a copy
*  of this software and associated documentation files (the "Software"), to deal
*  in the Software without restriction, including without limitation the rights
*  to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
*  copies of the Software, and to permit persons to whom the Software is
*  furnished to do so, subject to the following conditions:
*
*  The above copyright notice and this permission notice shall be included in all
*  copies or substantial portions of the Software.
*
*  THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
*  THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
*  IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
*  FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
*  AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
*  LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
*  OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
*  SOFTWARE.
*
*
*/


#ifndef API_GEARMAN_WORKER_CXX
#define API_GEARMAN_WORKER_CXX

#include <cstdint> /* uint32_t */
#include <cstring> /* strncpy */
#include <string>
#include <unordered_map>
#include <libgearman/gearman.h>

#define DEGUG_GEARMAN_CXX true
typedef std::string (GearmanCxxTask_t)  (bool &jobStatus, std::string &data);
typedef void* (GearmanWorkerFunction_t) (gearman_job_st *job, void *context, 
                                          size_t *result_size, gearman_return_t *ret_ptr);

class GearmanCxxWorker {
private:
  gearman_worker_st m_gmWorker_;
  gearman_return_t  m_gmReturn;

  std::string m_gmHost = "127.0.0.1";
  in_port_t m_gmPort = 4730;
  bool m_gmConnectionStatus;

  static std::unordered_map<std::string, GearmanCxxTask_t*> m_registeredTasks;
  
  typedef void* (GearmanCxxWorker::*executeTaskPtr_t) (
    gearman_job_st *job, void *context, size_t *result_size, gearman_return_t *ret_ptr);

  executeTaskPtr_t executeTaskPtr;
  //GearmanWorkerFunction_t executeTaskPtr;
  

public:
  GearmanCxxWorker();
  GearmanCxxWorker(std::string &host);
  GearmanCxxWorker(std::string &host, int &port);
  ~GearmanCxxWorker(void);
  void init(std::string &host, int &port);
  bool gearmanConnIsInvalid();
  bool connectToGearmanServer();
  bool registerTask(std::string &taskname, GearmanCxxTask_t *task);
  void startWorker();

  static void *executeTask(
    gearman_job_st *job, void *context, size_t *result_size, gearman_return_t *ret_ptr);
};


std::unordered_map<std::string, GearmanCxxTask_t*> GearmanCxxWorker::m_registeredTasks;

GearmanCxxWorker::GearmanCxxWorker() {}

GearmanCxxWorker::GearmanCxxWorker(std::string &host): m_gmHost(host) {
  if (connectToGearmanServer())
    m_gmConnectionStatus = true;
  else
    m_gmConnectionStatus = false;
}

GearmanCxxWorker::GearmanCxxWorker(std::string &host, int &port):
  m_gmHost(host),
  m_gmPort((in_port_t)port) {
  if (connectToGearmanServer())
    m_gmConnectionStatus = true;
  else
    m_gmConnectionStatus = false;
}

GearmanCxxWorker::~GearmanCxxWorker(void) {
  gearman_worker_free(&m_gmWorker_);
}

void GearmanCxxWorker::init(std::string &host, int &port) {
  m_gmHost = host;
  m_gmPort = (in_port_t)port;

  if (connectToGearmanServer())
    m_gmConnectionStatus = true;
  else
    m_gmConnectionStatus = false;
}

bool GearmanCxxWorker::gearmanConnIsInvalid() {
  if (m_gmConnectionStatus)
    return false;

  return true;
}

bool GearmanCxxWorker::connectToGearmanServer() {
  if (gearman_worker_create(&m_gmWorker_) == NULL) {
    #ifdef DEGUG_GEARMAN_CXX
      std::cout << "GEARMAN_ERROR: Memory allocation failure" << std::endl;
    #endif
    return false;
  }

  m_gmReturn = gearman_worker_add_server(&m_gmWorker_, m_gmHost.c_str(), m_gmPort);
  if (m_gmReturn != GEARMAN_SUCCESS) {
    #ifdef DEGUG_GEARMAN_CXX
      std::cout << std::string(gearman_worker_error(&m_gmWorker_)) << std::endl;
    #endif
    return false;
  }

  #ifdef DEGUG_GEARMAN_CXX
    std::cout << "Gearman Worker Connected" << std::endl;
  #endif

  //executeTaskPtr = &GearmanCxxWorker::executeTask;
  return true;
}

void *GearmanCxxWorker::executeTask(gearman_job_st *job, 
  void *context, size_t *result_size, gearman_return_t *ret_ptr) {

  std::string taskname = std::string(gearman_job_function_name(job));
  std::string data = std::string((char*)gearman_job_workload(job));
  bool jobStatus;
  std::string result_str = GearmanCxxWorker::m_registeredTasks[taskname](jobStatus, data);

  if(!jobStatus) {
    *ret_ptr= GEARMAN_WORK_FAIL;
    return NULL;
  }

  uint8_t *result;
  *result_size= result_str.size();

  result= (uint8_t *)malloc(*result_size);
  if (result == NULL) {
    #ifdef DEGUG_GEARMAN_CXX
      std::cout << "Malloc failed ..." << std::endl;
    #endif
    *ret_ptr= GEARMAN_WORK_FAIL;
    return NULL;
  }

  *ret_ptr= GEARMAN_SUCCESS;
  strncpy((char*)result, result_str.c_str(), *result_size);
  return result;
}

bool GearmanCxxWorker::registerTask(std::string &taskname, GearmanCxxTask_t *task) {
  // create gearman function to work with @taskname
  GearmanCxxWorker::m_registeredTasks[taskname] = task;

  //register function
  m_gmReturn = gearman_worker_add_function(
    &m_gmWorker_, 
    taskname.c_str(), 0, GearmanCxxWorker::executeTask, NULL);
  
  if (m_gmReturn != GEARMAN_SUCCESS) {
    #ifdef DEGUG_GEARMAN_CXX
      std::cout << std::string(gearman_worker_error(&m_gmWorker_)) << std::endl;
    #endif
    return false;
  }
  return true;
}

void GearmanCxxWorker::startWorker() {
  std::cout << "Worker Started ..." <<std::endl;
  while (1) {
    m_gmReturn= gearman_worker_work(&m_gmWorker_);
    if (m_gmReturn != GEARMAN_SUCCESS) {
      #ifdef DEGUG_GEARMAN_CXX
        std::cout << std::string(gearman_worker_error(&m_gmWorker_)) << std::endl;
      #endif
      break;
    }
  }
}

#endif
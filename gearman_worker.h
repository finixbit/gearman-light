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
#include <string>
#include <libgearman/gearman.h>

#define DEGUG_GEARMAN_CXX true
typedef std::string (GearmanCxxTask_t)   (bool &jobStatus, std::string &data);
typedef void* (*GearmanWorkerFunction_t) (gearman_job_st *job, void *context, 
                                          size_t *result_size, gearman_return_t *ret_ptr);

class GearmanCxxWorker {
private:
  GearmanWorkerFunction_t newGmFunction(GearmanCxxTask_t &task);

  gearman_worker_st gmWorker_;
  gearman_return_t  gmReturn;

  std::string gmHost = "127.0.0.1";
  in_port_t gmPort = 4730;
  bool gmConnectionStatus;

public:
  GearmanCxxWorker();
  GearmanCxxWorker(std::string &host);
  GearmanCxxWorker(std::string &host, int &port);
  ~GearmanCxxWorker(void);
  void init(std::string &host, int &port);
  bool gearmanConnIsInvalid();
  bool connectToGearmanServer();
  bool registerTask(std::string &taskname, GearmanCxxTask_t &task);
  void startWorker();
};

GearmanCxxWorker::GearmanCxxWorker() {}

GearmanCxxWorker::GearmanCxxWorker(std::string &host): gmHost(host) {
  if (connectToGearmanServer())
    gmConnectionStatus = true;
  else
    gmConnectionStatus = false;
}

GearmanCxxWorker::GearmanCxxWorker(std::string &host, int &port):
  gmHost(host),
  gmPort((in_port_t)port) {
  if (connectToGearmanServer())
    gmConnectionStatus = true;
  else
    gmConnectionStatus = false;
}

GearmanCxxWorker::~GearmanCxxWorker(void) {
  gearman_worker_free(&gmWorker_);
}

void GearmanCxxWorker::init(std::string &host, int &port) {
  gmHost = host;
  gmPort = (in_port_t)port;

  if (connectToGearmanServer())
    gmConnectionStatus = true;
  else
    gmConnectionStatus = false;
}

bool GearmanCxxWorker::gearmanConnIsInvalid() {
  if (gmConnectionStatus)
    return false;

  return true;
}

bool GearmanCxxWorker::connectToGearmanServer() {
  if (gearman_worker_create(&gmWorker_) == NULL) {
    #ifdef DEGUG_GEARMAN_CXX
      std::cout << "GEARMAN_ERROR: Memory allocation failure" << std::endl;
    #endif
    return false;
  }

  gmReturn = gearman_worker_add_server(&gmWorker_, gmHost.c_str(), gmPort);
  if (gmReturn != GEARMAN_SUCCESS) {
    #ifdef DEGUG_GEARMAN_CXX
      std::cout << std::string(gearman_worker_error(&gmWorker_)) << std::endl;
    #endif
    return false;
  }

  #ifdef DEGUG_GEARMAN_CXX
    std::cout << "Gearman Worker Connected" << std::endl;
  #endif

  return true;
}

GearmanWorkerFunction_t GearmanCxxWorker::newGmFunction(GearmanCxxTask_t &task) {
  GearmanWorkerFunction_t workerFunction = NULL;
  return workerFunction;
}

bool GearmanCxxWorker::registerTask(std::string &taskname, GearmanCxxTask_t &task) {
  // create gearman function to work with @taskname
  GearmanWorkerFunction_t workerFunction = newGmFunction(task);

  //register function
  gmReturn = gearman_worker_add_function(
    &gmWorker_, taskname.c_str(), 0, workerFunction, NULL);
  
  if (gmReturn != GEARMAN_SUCCESS) {
    #ifdef DEGUG_GEARMAN_CXX
      std::cout << std::string(gearman_worker_error(&gmWorker_)) << std::endl;
    #endif
    return false;
  }
  return true;
}

void GearmanCxxWorker::startWorker() {
  while (1) {
    std::cout << "Worker Started ..." <<std::endl;

    gmReturn= gearman_worker_work(&gmWorker_);
    if (gmReturn != GEARMAN_SUCCESS) {
      #ifdef DEGUG_GEARMAN_CXX
        std::cout << std::string(gearman_worker_error(&gmWorker_)) << std::endl;
      #endif
      break;
    }
  }
}

#endif
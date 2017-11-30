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

#ifndef API_GEARMAN_CXX
#define API_GEARMAN_CXX

#include <cstdint> /* uint32_t */
#include <string>
#include <libgearman/gearman.h>

#define DEGUG_GEARMAN_CXX true


class GearmanCxxClient {
private:
  gearman_client_st gmClient_;
  gearman_return_t  gmReturn;

  char gmJobHandle[GEARMAN_JOB_HANDLE_SIZE];
  char* gmResult;
  size_t gmResultSize;

  std::string gmHost;
  in_port_t gmPort;
  bool gmConnectionStatus;

public:
  GearmanCxxClient();
  GearmanCxxClient(std::string &host, int &port);
  ~GearmanCxxClient(void);
  void init(std::string &host, int &port);
  bool gearmanConnIsInvalid();
  bool connectToGearmanServer();
  bool gearmanSendJobBackground(std::string &task, std::string &data);
  std::string gearmanSendJob(
    std::string &task, std::string &data, bool waitTillComplete);
};

GearmanCxxClient::GearmanCxxClient() {}

GearmanCxxClient::GearmanCxxClient(std::string &host, int &port):
  gmHost(host),
  gmPort((in_port_t)port) {
  if (connectToGearmanServer())
    gmConnectionStatus = true;
  else
    gmConnectionStatus = false;
}

GearmanCxxClient::~GearmanCxxClient(void) {
  gearman_client_free(&gmClient_);
}

void GearmanCxxClient::init(std::string &host, int &port) {
  gmHost = host;
  gmPort = (in_port_t)port;

  if (connectToGearmanServer())
    gmConnectionStatus = true;
  else
    gmConnectionStatus = false;
}

bool GearmanCxxClient::gearmanConnIsInvalid() {
  if (gmConnectionStatus)
    return false;

  return true;
}

bool GearmanCxxClient::connectToGearmanServer() {
  if (gearman_client_create(&gmClient_) == NULL) {
    #ifdef DEGUG_GEARMAN_CXX
      std::cout << "GEARMAN_ERROR: Memory allocation failure" << std::endl;
    #endif
    return false;
  }

  gmReturn = gearman_client_add_server(&gmClient_, gmHost.c_str(), gmPort);
  if (gmReturn != GEARMAN_SUCCESS) {
    #ifdef DEGUG_GEARMAN_CXX
      std::cout << std::string(gearman_client_error(&gmClient_)) << std::endl;
    #endif
    return false;
  }

  #ifdef DEGUG_GEARMAN_CXX
    std::cout << "Gearman Client Connected" << std::endl;
  #endif

  return true;
}

bool GearmanCxxClient::gearmanSendJobBackground(
  std::string &task, std::string &data) {
  gmReturn = gearman_client_do_background(&gmClient_,
                                          task.c_str(),
                                          NULL,
                                          (void *)data.c_str(),
                                          (size_t)data.length(),
                                          gmJobHandle);

  if (gmReturn != GEARMAN_SUCCESS) {
    #ifdef DEGUG_GEARMAN_CXX
      std::cout << std::string(gearman_client_error(&gmClient_)) << std::endl;
    #endif
    return false;
  }

  return true;
}

std::string GearmanCxxClient::gearmanSendJob(std::string &task, std::string &data) {
  gmResult = (char *)gearman_client_do(&gmClient_,
                                       task.c_str(),
                                       NULL,
                                       (void *)data.c_str(),
                                       (size_t)data.length(),
                                       &gmResultSize,
                                       &gmReturn);

  while (1) {
    if (gmReturn == GEARMAN_SUCCESS)
      break;

    if (gmReturn == GEARMAN_WORK_FAIL) {
      #ifdef DEGUG_GEARMAN_CXX
        std::cout << std::string(gearman_client_error(&gmClient_)) << std::endl;
      #endif
      return std::string("CACHE_INVALID_PARAM");
    }
  }

  std::string result;
  result.append(gmResult, gmResultSize);
  return result;
}


#endif // API_GEARMAN_CXX
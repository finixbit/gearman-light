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

#include <iostream>
#include "../gearman_worker.h"


std::string reverseJob(bool &jobStatus, std::string &data);

int main(int argc, char* argv[]) {

  std::string gearmanHost("120.0.0.1");
  int gearmanPort = 4730;

  GearmanCxxWorker gmWorker(gearmanHost, gearmanPort);

  if(gmWorker.gearmanConnIsInvalid()) {
    std::cout << "Connection Failed ..." << std::endl;
    return 1;
  }

  // register task
  std::string gearmanTaskName("reverse_str");
  bool response = gmWorker.registerTask(gearmanTaskName, reverseJob);
  if(response)
    std::cout << "job registered successfully ..." << std::endl;

  gmWorker.startWorker();
  return 0;
}

std::string reverseJob(bool &jobStatus, std::string &data) {
  jobStatus = true;
  return std::string("helloworld");
}
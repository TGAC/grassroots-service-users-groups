
/*
** Copyright 2014-2018 The Earlham Institute
**
** Licensed under the Apache License, Version 2.0 (the "License");
** you may not use this file except in compliance with the License.
** You may obtain a copy of the License at
**
**     http://www.apache.org/licenses/LICENSE-2.0
**
** Unless required by applicable law or agreed to in writing, software
** distributed under the License is distributed on an "AS IS" BASIS,
** WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
** See the License for the specific language governing permissions and
** limitations under the License.
*/
/*
 * users_submission_service.h
 *
 *  Created on: 11 Dec 2023
 *      Author: billy
 */



#ifndef SERVICES_USERS_SERVICE_INCLUDE_USERS_SUBMISSION_SERVICE_H_
#define SERVICES_USERS_SERVICE_INCLUDE_USERS_SUBMISSION_SERVICE_H_



#include "users_service_data.h"
#include "users_service_library.h"



#ifdef __cplusplus
extern "C"
{
#endif


USERS_SERVICE_LOCAL Service *GetUsersSubmissionService (GrassrootsServer *grassroots_p);


#ifdef __cplusplus
}
#endif


#endif /* SERVICES_USERS_SERVICE_INCLUDE_USERS_SUBMISSION_SERVICE_H_ */

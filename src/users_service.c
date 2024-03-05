/*
 ** Copyright 2014-2016 The Earlham Institute
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
#include <string.h>

#include "jansson.h"

#define ALLOCATE_USERS_SERVICE_TAGS (1)
#include "users_service.h"

#include "memory_allocations.h"
#include "parameter.h"
#include "service_job.h"
#include "string_utils.h"
#include "json_tools.h"
#include "grassroots_server.h"
#include "string_linked_list.h"
#include "math_utils.h"
#include "search_options.h"
#include "time_util.h"
#include "io_utils.h"
#include "audit.h"


#include "users_submission_service.h"


#ifdef _DEBUG
#define USERS_SERVICE_DEBUG	(STM_LEVEL_FINER)
#else
#define USERS_SERVICE_DEBUG	(STM_LEVEL_NONE)
#endif



/*
 * STATIC PROTOTYPES
 */


/*
 * API FUNCTIONS
 */


ServicesArray *GetServices (User *user_p, GrassrootsServer *grassroots_p)
{
	Service *users_submission_service_p = GetUsersSubmissionService (grassroots_p);

	if (users_submission_service_p)
		{
			ServicesArray *services_p = AllocateServicesArray (1);

			if (services_p)
				{
					* (services_p -> sa_services_pp) = users_submission_service_p;

					return services_p;
				}

			FreeService (users_submission_service_p);
		}


	return NULL;
}


void ReleaseServices (ServicesArray *services_p)
{
	FreeServicesArray (services_p);
}


/*
 * users_service.h
 *
 *  Created on: 8 Dec 2023
 *      Author: billy
 */

#ifndef SERVICES_USERS_SERVICE_INCLUDE_USERS_SERVICE_H_
#define SERVICES_USERS_SERVICE_INCLUDE_USERS_SERVICE_H_

#include "users_service_library.h"
#include "service.h"
#include "schema_keys.h"


#ifdef __cplusplus
extern "C"
{
#endif


/**
 * Get the Service available for running the Users Submission service
 *
 * @param user_p The User for the user trying to access the services.
 * This can be <code>NULL</code>.
 * @return The ServicesArray containing the Users Submission service or
 * <code>NULL</code> upon error.
 *
 */
USERS_SERVICE_API ServicesArray *GetServices (User *user_p, GrassrootsServer *grassroots_p);


/**
 * Free the ServicesArray and its associated  Users Submission service.
 *
 * @param services_p The ServicesArray to free.
 *
 */
USERS_SERVICE_API void ReleaseServices (ServicesArray *services_p);


#ifdef __cplusplus
}
#endif



#endif /* SERVICES_USERS_SERVICE_INCLUDE_USERS_SERVICE_H_ */

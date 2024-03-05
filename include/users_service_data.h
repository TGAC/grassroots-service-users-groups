/*
 * users_service_data.h
 *
 *  Created on: 8 Dec 2023
 *      Author: billy
 */

#ifndef SERVICES_USERS_SERVICE_INCLUDE_USERS_SERVICE_DATA_H_
#define SERVICES_USERS_SERVICE_INCLUDE_USERS_SERVICE_DATA_H_

#include "mongodb_tool.h"

#include "users_service.h"

/**
 * The configuration data used by the Users Service.
 *
 * @extends ServiceData
 */
typedef struct /*USERS_SERVICE_LOCAL*/ UsersServiceData
{
	/** The base ServiceData. */
	ServiceData usd_base_data;


	/**
	 * @private
	 *
	 * The MongoTool to connect to the database where our data is stored.
	 */
	MongoTool *usd_mongo_p;


	/**
	 * @private
	 *
	 * The name of the database to use.
	 */
	const char *usd_database_s;


	/**
	 * @private
	 *
	 * The collection name to use for the Users data
	 */
	const char *usd_users_collection_s;

	/**
	 * @private
	 *
	 * The collection name to use for the Groups data
	 */
	const char *usd_groups_collection_s;

} UsersServiceData;

/** The prefix to use for Field Trial Service aliases. */
#define US_GROUP_ALIAS_PREFIX_S "users_and_groups"

#ifndef DOXYGEN_SHOULD_SKIP_THIS

#ifdef ALLOCATE_USERS_SERVICE_TAGS
	#define USERS_PREFIX USERS_SERVICE_LOCAL
	#define USERS_VAL(x)	= x
	#define USERS_CONCAT_VAL(x,y) = x y
#else
	#define USERS_PREFIX extern
	#define USERS_VAL(x)
	#define USERS_CONCAT_VAL(x,y) = x y
#endif

#endif 		/* #ifndef DOXYGEN_SHOULD_SKIP_THIS */


#ifdef __cplusplus
extern "C"
{
#endif

USERS_SERVICE_LOCAL UsersServiceData *AllocateUsersServiceData (void);


USERS_SERVICE_LOCAL void FreeUsersServiceData (UsersServiceData *data_p);


USERS_SERVICE_LOCAL bool ConfigureUsersService (UsersServiceData *data_p, GrassrootsServer *grassroots_p);

#ifdef __cplusplus
}
#endif

#endif /* SERVICES_USERS_SERVICE_INCLUDE_USERS_SERVICE_DATA_H_ */

/*
 * users_service_data.c
 *
 *  Created on: 8 Dec 2023
 *      Author: billy
 */

#include "users_service_data.h"



#include "streams.h"
#include "string_utils.h"


UsersServiceData *AllocateUsersServiceData  (void)
{
	UsersServiceData *data_p = (UsersServiceData *) AllocMemory (sizeof (UsersServiceData));

	if (data_p)
		{
			data_p -> usd_mongo_p = NULL;
			data_p -> usd_database_s = NULL;
			data_p -> usd_users_collection_s = NULL;
			data_p -> usd_groups_collection_s = NULL;

			return data_p;
		}

	return NULL;
}


void FreeUsersServiceData (UsersServiceData *data_p)
{
	if (data_p -> usd_mongo_p)
		{
			FreeMongoTool (data_p -> usd_mongo_p);
		}

	FreeMemory (data_p);
}


bool ConfigureUsersService (UsersServiceData *data_p, GrassrootsServer *grassroots_p)
{
	bool success_flag = false;
	const json_t *service_config_p = data_p -> usd_base_data.sd_config_p;

	data_p -> usd_database_s = GetJSONString (service_config_p, "database");

	if (data_p -> usd_database_s)
		{
			if ((data_p -> usd_users_collection_s = GetJSONString (service_config_p, "users_collection")) != NULL)
				{
					if ((data_p -> usd_groups_collection_s = GetJSONString (service_config_p, "groups_collection")) != NULL)
						{
							if ((data_p -> usd_mongo_p = AllocateMongoTool (NULL, grassroots_p -> gs_mongo_manager_p)) != NULL)
								{
									if (SetMongoToolDatabase (data_p -> usd_mongo_p, data_p -> usd_database_s))
										{
											success_flag = true;
										}
									else
										{
											PrintErrors (STM_LEVEL_SEVERE, __FILE__, __LINE__, "Failed to set MongoTool database to \"%s\"", data_p -> usd_database_s);
										}


								}		/* if ((data_p -> usd_mongo_p = AllocateMongoTool (NULL, grassroots_p -> gs_mongo_manager_p)) != NULL) */
							else
								{
									PrintErrors (STM_LEVEL_SEVERE, __FILE__, __LINE__, "Failed to allocate MongoTool");
								}

						}		/* if ((data_p -> usd_groups_collection_s = GetJSONString (service_config_p, "groups_collection")) != NULL) */

				}		/* if ((data_p -> usd_users_collection_s = GetJSONString (service_config_p, "users_collection")) != NULL) */

		}		/* if (data_p -> psd_database_s) */

	return success_flag;
}



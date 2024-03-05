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
 * submission_service.c
 *
 *  Created on: 8 Dec 2023
 *      Author: billy
 */

#include <string.h>

#include "users_submission_service.h"
#include "users_service.h"
#include "users_service_data.h"


#include "audit.h"
#include "streams.h"
#include "math_utils.h"
#include "string_utils.h"
#include "schema_keys.h"
#include "mongodb_util.h"

#include "string_parameter.h"

/*
 * Static declarations
 */

static NamedParameterType S_USER_ID = { "US Id", PT_STRING };


static NamedParameterType S_EMAIL = { "US Email", PT_STRING };
static NamedParameterType S_SURNAME = { "US Surname", PT_STRING };
static NamedParameterType S_FORENAME = { "US Forename", PT_STRING };
static NamedParameterType S_AFFILIATION = { "US Affiliation", PT_STRING };
static NamedParameterType S_ORCID = { "US ORCID", PT_STRING };

static const char * const S_EMPTY_LIST_OPTION_S = "<empty>";



static const char *GetUsersSubmissionServiceName (const Service *service_p);

static const char *GetUsersSubmissionServiceDescription (const Service *service_p);

static const char *GetUsersSubmissionServiceAlias (const Service *service_p);

static const char *GetUsersSubmissionServiceInformationUri (const Service *service_p);

static ParameterSet *GetUsersSubmissionServiceParameters (Service *service_p, DataResource *resource_p, User *user_p);

static void ReleaseUsersSubmissionServiceParameters (Service *service_p, ParameterSet *params_p);

static ServiceJobSet *RunUsersSubmissionService (Service *service_p, ParameterSet *param_set_p, User *user_p, ProvidersStateTable *providers_p);

static ParameterSet *IsResourceForUsersSubmissionService (Service *service_p, DataResource *resource_p, Handler *handler_p);

static bool CloseUsersSubmissionService (Service *service_p);

static ServiceMetadata *GetUsersSubmissionServiceMetadata (Service *service_p);

static bool GetUsersSubmissionServiceParameterTypesForNamedParameters (const Service *service_p, const char *param_name_s, ParameterType *pt_p);


static bool SetUpUsersListParameter (const UsersServiceData *data_p, Parameter *param_p, const User *active_user_p, const bool empty_option_flag);


static json_t *GetAllUsersAsJSON (const UsersServiceData *data_p, const bool full_data_flag);


static OperationStatus SaveUser (User *user_p, ServiceJob *job_p, UsersServiceData *data_p);

//static User *GetUserByIdString (const char *user_id_s, const UsersServiceData *data_p);

//static User *GetUserByNamedId (const bson_oid_t *id_p, const char * const collection_s, const char *id_key_s, const UsersServiceData *data_p);

static User *GetUserFromResource (DataResource *resource_p, const NamedParameterType program_param_type, UsersServiceData *dfw_data_p);

static bool SetUpDefaultsFromExistingUser (const User *user_p, char **id_ss);


/*
 * API definitions
 */


Service *GetUsersSubmissionService (GrassrootsServer *grassroots_p)
{
	Service *service_p = (Service *) AllocMemory (sizeof (Service));

	if (service_p)
		{
			UsersServiceData *data_p = AllocateUsersServiceData ();

			if (data_p)
				{
					if (InitialiseService (service_p,
																 GetUsersSubmissionServiceName,
																 GetUsersSubmissionServiceDescription,
																 GetUsersSubmissionServiceAlias,
																 GetUsersSubmissionServiceInformationUri,
																 RunUsersSubmissionService,
																 IsResourceForUsersSubmissionService,
																 GetUsersSubmissionServiceParameters,
																 GetUsersSubmissionServiceParameterTypesForNamedParameters,
																 ReleaseUsersSubmissionServiceParameters,
																 CloseUsersSubmissionService,
																 NULL,
																 false,
																 SY_SYNCHRONOUS,
																 (ServiceData *) data_p,
																 GetUsersSubmissionServiceMetadata,
																 NULL,
																 grassroots_p))
						{

							if (ConfigureUsersService (data_p, grassroots_p))
								{
									return service_p;
								}
						}		/* if (InitialiseService (.... */
					else
						{
							FreeUsersServiceData (data_p);
						}

				}		/* if (data_p) */

			FreeService (service_p);
		}		/* if (service_p) */

	return NULL;
}



static const char *GetUsersSubmissionServiceName (const Service * UNUSED_PARAM (service_p))
{
	return "Users submission service";
}


static const char *GetUsersSubmissionServiceDescription (const Service * UNUSED_PARAM (service_p))
{
	return "A service to submit Users";
}


static const char *GetUsersSubmissionServiceAlias (const Service * UNUSED_PARAM (service_p))
{
	return US_GROUP_ALIAS_PREFIX_S SERVICE_GROUP_ALIAS_SEPARATOR "submit_user";
}


static const char *GetUsersSubmissionServiceInformationUri (const Service * UNUSED_PARAM (service_p))
{
	return NULL;
}


static ParameterSet *GetUsersSubmissionServiceParameters (Service *service_p, DataResource *resource_p, User * UNUSED_PARAM (user_p))
{
	ParameterSet *param_set_p = AllocateParameterSet ("User submission service parameters", "The parameters used for the User submission service");

	if (param_set_p)
		{
			ServiceData *data_p = service_p -> se_data_p;
			UsersServiceData *us_data_p = (UsersServiceData *) data_p;
			Parameter *param_p = NULL;
			ParameterGroup *group_p = CreateAndAddParameterGroupToParameterSet ("User details", false, data_p, param_set_p);
			char *id_s = NULL;
			User *active_user_p = GetUserFromResource (resource_p, S_USER_ID, us_data_p);
			bool defaults_flag = false;


			if (active_user_p)
				{
					if (SetUpDefaultsFromExistingUser (active_user_p, &id_s))
						{
							defaults_flag = true;
						}
				}
			else
				{
					id_s = (char *) S_EMPTY_LIST_OPTION_S;
					defaults_flag = true;
				}

			if ((param_p = EasyCreateAndAddStringParameterToParameterSet (data_p, param_set_p, group_p, S_USER_ID.npt_type, S_USER_ID.npt_name_s, "Load User", "Edit an existing User", id_s, PL_ALL)) != NULL)
				{
					if (SetUpUsersListParameter (us_data_p, param_p, active_user_p, true))
						{
							/*
							 * We want to update all of the values in the form
							 * when a user selects a study from the list so
							 * we need to make the parameter automatically
							 * refresh the values. So we set the
							 * pa_refresh_service_flag to true.
							 */
							param_p -> pa_refresh_service_flag = true;



							if ((param_p = EasyCreateAndAddStringParameterToParameterSet (data_p, param_set_p, group_p, S_EMAIL.npt_type, S_EMAIL.npt_name_s, "Email", "The user's email address", active_user_p ? active_user_p -> us_email_s : NULL, PL_ALL)) != NULL)
								{
									param_p -> pa_required_flag = true;

									if ((param_p = EasyCreateAndAddStringParameterToParameterSet (data_p, param_set_p, group_p, S_SURNAME.npt_type, S_SURNAME.npt_name_s, "Last name", "The User's last name", active_user_p ? active_user_p -> us_surname_s : NULL, PL_ALL)) != NULL)
										{
											param_p -> pa_required_flag = true;

											if ((param_p = EasyCreateAndAddStringParameterToParameterSet (data_p, param_set_p, group_p, S_FORENAME.npt_type, S_FORENAME.npt_name_s, "First name", "The User's first name", active_user_p ? active_user_p -> us_forename_s : NULL, PL_ALL)) != NULL)
												{
													param_p -> pa_required_flag = true;

													if ((param_p = EasyCreateAndAddStringParameterToParameterSet (data_p, param_set_p, group_p, S_AFFILIATION.npt_type, S_AFFILIATION.npt_name_s, "Affiliation", "The orgranisation that the user belongs to", active_user_p ? active_user_p -> us_org_s : NULL, PL_ALL)) != NULL)
														{
															if ((param_p = EasyCreateAndAddStringParameterToParameterSet (data_p, param_set_p, group_p, S_ORCID.npt_type, S_ORCID.npt_name_s, "ORCID", "The user's ORCID", active_user_p ? active_user_p -> us_orcid_s : NULL, PL_ALL)) != NULL)
																{
																	return param_set_p;
																}
															else
																{
																	PrintErrors (STM_LEVEL_SEVERE, __FILE__, __LINE__, "Failed to add %s parameter", S_ORCID.npt_name_s);
																}


														}
													else
														{
															PrintErrors (STM_LEVEL_SEVERE, __FILE__, __LINE__, "Failed to add %s parameter", S_AFFILIATION.npt_name_s);
														}

												}
											else
												{
													PrintErrors (STM_LEVEL_SEVERE, __FILE__, __LINE__, "Failed to add %s parameter", S_FORENAME.npt_name_s);
												}
										}
									else
										{
											PrintErrors (STM_LEVEL_SEVERE, __FILE__, __LINE__, "Failed to add %s parameter", S_SURNAME.npt_name_s);
										}

								}
							else
								{
									PrintErrors (STM_LEVEL_SEVERE, __FILE__, __LINE__, "Failed to add %s parameter", S_EMAIL.npt_name_s);
								}

						}		/* if (SetUpUsersListParameter ((UsersServiceData *) data_p, (StringParameter *) param_p, active_user_p, true)) */

				}		/* if ((param_p = EasyCreateAndAddStringParameterToParameterSet (data_p, param_set_p, group_p, S_USER_ID.npt_type, S_USER_ID.npt_name_s, "Load User", "Edit an existing User", id_s, PL_ALL)) != NULL) */

			FreeParameterSet (param_set_p);
		}
	else
		{
			PrintErrors (STM_LEVEL_SEVERE, __FILE__, __LINE__, "Failed to allocate %s ParameterSet", GetUsersSubmissionServiceName (service_p));
		}

	return NULL;
}


static bool GetUsersSubmissionServiceParameterTypesForNamedParameters (const Service *service_p, const char *param_name_s, ParameterType *pt_p)
{
	bool success_flag = false;

	const NamedParameterType params [] =
		{
			S_EMAIL,
			S_SURNAME,
			S_FORENAME,
			S_AFFILIATION,
			S_ORCID,
			S_USER_ID,
			NULL
		};

	return DefaultGetParameterTypeForNamedParameter (param_name_s, pt_p, params);
}


static void ReleaseUsersSubmissionServiceParameters (Service * UNUSED_PARAM (service_p), ParameterSet *params_p)
{
	FreeParameterSet (params_p);
}




static bool CloseUsersSubmissionService (Service *service_p)
{
	bool success_flag = true;

	FreeUsersServiceData ((UsersServiceData *) (service_p -> se_data_p));;

	return success_flag;
}



static ServiceJobSet *RunUsersSubmissionService (Service *service_p, ParameterSet *param_set_p, User * UNUSED_PARAM (logged_in_user_p), ProvidersStateTable * UNUSED_PARAM (providers_p))
{
	UsersServiceData *data_p = (UsersServiceData *) (service_p -> se_data_p);

	service_p -> se_jobs_p = AllocateSimpleServiceJobSet (service_p, NULL, "Users");

	if (service_p -> se_jobs_p)
		{
			OperationStatus status = OS_FAILED_TO_START;
			ServiceJob *job_p = GetServiceJobFromServiceJobSet (service_p -> se_jobs_p, 0);

			LogParameterSet (param_set_p, job_p);

			if (param_set_p)
				{
					const char *email_s = NULL;

					if (GetCurrentStringParameterValueFromParameterSet (param_set_p, S_EMAIL.npt_name_s, &email_s))
						{
							if (!IsStringEmpty (email_s))
								{
									const char *surname_s = NULL;

									if (GetCurrentStringParameterValueFromParameterSet (param_set_p, S_SURNAME.npt_name_s, &surname_s))
										{
											if (!IsStringEmpty (surname_s))
												{
													const char *forename_s = NULL;

													if (GetCurrentStringParameterValueFromParameterSet (param_set_p, S_FORENAME.npt_name_s, &forename_s))
														{
															if (!IsStringEmpty (forename_s))
																{
																	User *user_p = NULL;
																	const char *orcid_s = NULL;
																	const char *affiliation_s = NULL;

																	GetCurrentStringParameterValueFromParameterSet (param_set_p, S_ORCID.npt_name_s, &orcid_s);
																	GetCurrentStringParameterValueFromParameterSet (param_set_p, S_AFFILIATION.npt_name_s, &affiliation_s);

																	user_p = AllocateUser (NULL, email_s, forename_s, surname_s, affiliation_s, orcid_s);

																	if (user_p)
																		{
																			status = SaveUser (user_p, job_p, data_p);

																			FreeUser (user_p);
																		}

																}
														}
												}
										}
								}
						}
				}		/* if (param_set_p) */

			SetServiceJobStatus (job_p, status);
			LogServiceJob (job_p);
		}		/* if (service_p -> se_jobs_p) */

	return service_p -> se_jobs_p;
}


static ServiceMetadata *GetUsersSubmissionServiceMetadata (Service *service_p)
{
	const char *term_url_s = CONTEXT_PREFIX_EDAM_ONTOLOGY_S "topic_0625";
	SchemaTerm *category_p = AllocateSchemaTerm (term_url_s, "Genotype and phenotype",
																							 "The study of genetic constitution of a living entity, such as an individual, and organism, a cell and so on, "
																							 "typically with respect to a particular observable phenotypic traits, or resources concerning such traits, which "
																							 "might be an aspect of biochemistry, physiology, morphology, anatomy, development and so on.");

	if (category_p)
		{
			SchemaTerm *subcategory_p;

			term_url_s = CONTEXT_PREFIX_EDAM_ONTOLOGY_S "operation_0304";
			subcategory_p = AllocateSchemaTerm (term_url_s, "Query and retrieval", "Search or query a data resource and retrieve entries and / or annotation.");

			if (subcategory_p)
				{
					ServiceMetadata *metadata_p = AllocateServiceMetadata (category_p, subcategory_p);

					if (metadata_p)
						{
							SchemaTerm *input_p;

							term_url_s = CONTEXT_PREFIX_EDAM_ONTOLOGY_S "data_0968";
							input_p = AllocateSchemaTerm (term_url_s, "Keyword",
																						"Boolean operators (AND, OR and NOT) and wildcard characters may be allowed. Keyword(s) or phrase(s) used (typically) for text-searching purposes.");

							if (input_p)
								{
									if (AddSchemaTermToServiceMetadataInput (metadata_p, input_p))
										{
											SchemaTerm *output_p;
											/* Genotype */
											term_url_s = CONTEXT_PREFIX_EXPERIMENTAL_FACTOR_ONTOLOGY_S "EFO_0000513";
											output_p = AllocateSchemaTerm (term_url_s, "genotype", "Information, making the distinction between the actual physical material "
																										 "(e.g. a cell) and the information about the genetic content (genotype).");

											if (output_p)
												{
													if (AddSchemaTermToServiceMetadataOutput (metadata_p, output_p))
														{
															return metadata_p;
														}		/* if (AddSchemaTermToServiceMetadataOutput (metadata_p, output_p)) */
													else
														{
															PrintErrors (STM_LEVEL_SEVERE, __FILE__, __LINE__, "Failed to add output term %s to service metadata", term_url_s);
															FreeSchemaTerm (output_p);
														}

												}		/* if (output_p) */
											else
												{
													PrintErrors (STM_LEVEL_SEVERE, __FILE__, __LINE__, "Failed to allocate output term %s for service metadata", term_url_s);
												}

										}		/* if (AddSchemaTermToServiceMetadataInput (metadata_p, input_p)) */
									else
										{
											PrintErrors (STM_LEVEL_SEVERE, __FILE__, __LINE__, "Failed to add input term %s to service metadata", term_url_s);
											FreeSchemaTerm (input_p);
										}

								}		/* if (input_p) */
							else
								{
									PrintErrors (STM_LEVEL_SEVERE, __FILE__, __LINE__, "Failed to allocate input term %s for service metadata", term_url_s);
								}

						}		/* if (metadata_p) */
					else
						{
							PrintErrors (STM_LEVEL_SEVERE, __FILE__, __LINE__, "Failed to allocate service metadata");
						}

				}		/* if (subcategory_p) */
			else
				{
					PrintErrors (STM_LEVEL_SEVERE, __FILE__, __LINE__, "Failed to allocate sub-category term %s for service metadata", term_url_s);
				}

		}		/* if (category_p) */
	else
		{
			PrintErrors (STM_LEVEL_SEVERE, __FILE__, __LINE__, "Failed to allocate category term %s for service metadata", term_url_s);
		}

	return NULL;
}



static ParameterSet *IsResourceForUsersSubmissionService (Service * UNUSED_PARAM (service_p), DataResource * UNUSED_PARAM (resource_p), Handler * UNUSED_PARAM (handler_p))
{
	return NULL;
}



static json_t *GetAllUsersAsJSON (const UsersServiceData *data_p, const bool full_data_flag)
{
	json_t *results_p = NULL;

	if (SetMongoToolCollection (data_p -> usd_mongo_p, data_p -> usd_users_collection_s))
		{
			bson_t *query_p = NULL;
			bson_t *opts_p = NULL;

			if (full_data_flag)
				{
					opts_p =  BCON_NEW ( "sort", "{", US_SURNAME_S, BCON_INT32 (1), US_FORENAME_S, BCON_INT32 (1), "}");
				}
			else
				{
					opts_p =  BCON_NEW ("projection", "{", US_SURNAME_S, BCON_BOOL (true), US_FORENAME_S, BCON_BOOL (true), "}",
															"sort", "{", US_SURNAME_S, BCON_INT32 (1), US_FORENAME_S, BCON_INT32 (1), "}");
				}


			results_p = GetAllMongoResultsAsJSON (data_p -> usd_mongo_p, query_p, opts_p);

			if (opts_p)
				{
					bson_destroy (opts_p);
				}

		}		/* if (SetMongoToolCollection (data_p -> dftsd_mongo_p, data_p -> dftsd_collection_ss [DFTD_PROGRAMME])) */

	return results_p;
}


static bool SetUpUsersListParameter (const UsersServiceData *data_p, Parameter *param_p, const User *active_user_p, const bool empty_option_flag)
{
	bool success_flag = false;
	json_t *results_p = GetAllUsersAsJSON (data_p, true);
	bool value_set_flag = false;

	if (results_p)
		{
			if (json_is_array (results_p))
				{
					const size_t num_results = json_array_size (results_p);

					success_flag = true;

					/*
					 * If there's an empty option, add it
					 */
					if (empty_option_flag)
						{
							success_flag = CreateAndAddStringParameterOption (param_p, S_EMPTY_LIST_OPTION_S, S_EMPTY_LIST_OPTION_S);
						}

					if (success_flag)
						{
							if (num_results > 0)
								{
									size_t i = 0;
									const char *param_value_s = GetStringParameterCurrentValue (param_p);

									bson_oid_t *id_p = GetNewUnitialisedBSONOid ();

									if (id_p)
										{
											while ((i < num_results) && success_flag)
												{
													json_t *entry_p = json_array_get (results_p, i);

													if (GetMongoIdFromJSON (entry_p, id_p))
														{
															char *id_s = GetBSONOidAsString (id_p);

															if (id_s)
																{
																	User *user_p = GetUserFromJSON (entry_p);

																	if (user_p)
																		{
																			char *name_s = GetFullUsername (user_p);

																			if (name_s)
																				{
																					if (param_value_s && (strcmp (param_value_s, id_s) == 0))
																						{
																							value_set_flag = true;
																						}

																					if (!CreateAndAddStringParameterOption (param_p, id_s, name_s))
																						{
																							success_flag = false;
																							PrintErrors (STM_LEVEL_SEVERE, __FILE__, __LINE__, "Failed to add param option \"%s\": \"%s\"", id_s, name_s);
																						}


																					FreeFullUsername (name_s);
																				}

																		}		/* if (name_s) */
																	else
																		{
																			success_flag = false;
																			PrintJSONToErrors (STM_LEVEL_SEVERE, __FILE__, __LINE__, entry_p, "Failed to get full username");
																		}

																	FreeBSONOidString (id_s);
																}
															else
																{
																	success_flag = false;
																	PrintJSONToErrors (STM_LEVEL_SEVERE, __FILE__, __LINE__, entry_p, "Failed to get Programme BSON oid");
																}

														}		/* if (GetMongoIdFromJSON (entry_p, id_p)) */
													else
														{
															success_flag = false;
															PrintJSONToErrors (STM_LEVEL_SEVERE, __FILE__, __LINE__, entry_p, "GetMongoIdFromJSON () failed");
														}

													if (success_flag)
														{
															++ i;
														}

												}		/* while ((i < num_results) && success_flag) */

											FreeBSONOid (id_p);
										}		/* if (id_p) */

									/*
									 * If the parameter's value isn't on the list, reset it
									 */
									if ((param_value_s != NULL) && (strcmp (param_value_s, S_EMPTY_LIST_OPTION_S) != 0) && (value_set_flag == false))
										{
											PrintErrors (STM_LEVEL_SEVERE, __FILE__, __LINE__, "param value \"%s\" not on list of existing programmes", param_value_s);
										}

								}		/* if (num_results > 0) */
							else
								{
									/* nothing to add */
									success_flag = true;
								}

						}		/* if (success_flag) */


				}		/* if (json_is_array (results_p)) */

			json_decref (results_p);
		}		/* if (results_p) */

	if (success_flag)
		{
			if (active_user_p)
				{
					char *id_s = GetBSONOidAsString (active_user_p -> us_id_p);

					if (id_s)
						{
							success_flag = SetStringParameterDefaultValue (param_p, id_s);
							FreeBSONOidString (id_s);
						}
					else
						{
							PrintErrors (STM_LEVEL_SEVERE, __FILE__, __LINE__, "Failed to get id string for active program \"%s\"", active_user_p -> us_email_s);
							success_flag = false;
						}
				}
		}

	return success_flag;
}




static OperationStatus SaveUser (User *user_p, ServiceJob *job_p, UsersServiceData *data_p)
{
	OperationStatus status = OS_FAILED;
	bson_t *selector_p = NULL;
	bool success_flag = PrepareSaveData (& (user_p -> us_id_p), &selector_p);

	if (success_flag)
		{
			json_t *user_json_p = GetUserAsJSON (user_p, true);

			if (user_json_p)
				{
					if (SaveMongoDataWithTimestamp (data_p -> usd_mongo_p, user_json_p, data_p -> usd_users_collection_s, selector_p, MONGO_TIMESTAMP_S))
						{
							status = OS_SUCCEEDED;
						}
					else
						{
							status = OS_FAILED;
						}


					json_decref (user_json_p);
				}


		}		/* if (success_flag) */

	SetServiceJobStatus (job_p, status);

	return status;
}


static User *GetUserFromResource (DataResource *resource_p, const NamedParameterType program_param_type, UsersServiceData *us_data_p)
{
	User *user_p = NULL;

	/*
	 * Have we been set some parameter values to refresh from?
	 */
	if (resource_p && (resource_p -> re_data_p))
		{
			const json_t *param_set_json_p = json_object_get (resource_p -> re_data_p, PARAM_SET_KEY_S);

			if (param_set_json_p)
				{
					json_t *params_json_p = json_object_get (param_set_json_p, PARAM_SET_PARAMS_S);

					if (params_json_p)
						{
							const char *user_id_s = GetNamedParameterDefaultValueFromJSON (program_param_type.npt_name_s, params_json_p);

							/*
							 * Do we have an existing user id?
							 */
							if (user_id_s)
								{
									GrassrootsServer *grassroots_p = us_data_p -> usd_base_data.sd_service_p -> se_grassroots_p;
									user_p = GetUserByIdString (grassroots_p, user_id_s);

									if (!user_p)
										{
											PrintJSONToErrors (STM_LEVEL_WARNING, __FILE__, __LINE__, params_json_p, "Failed to load User with id \"%s\"", user_id_s);
										}

								}		/* if (user_id_s) */

						}
					else
						{
							PrintJSONToErrors (STM_LEVEL_WARNING, __FILE__, __LINE__, param_set_json_p, "Failed to get params with key \"%s\"", PARAM_SET_PARAMS_S);
						}
				}
			else
				{
					PrintJSONToErrors (STM_LEVEL_WARNING, __FILE__, __LINE__, resource_p -> re_data_p, "Failed to get param set with key \"%s\"", PARAM_SET_KEY_S);
				}

		}		/* if (resource_p && (resource_p -> re_data_p)) */

	return user_p;
}


//static User *GetUserByIdString (const char *user_id_s, const UsersServiceData *data_p)
//{
//	User *user_p = NULL;
//
//	if (bson_oid_is_valid (user_id_s, strlen (user_id_s)))
//		{
//			bson_oid_t *id_p = GetBSONOidFromString (user_id_s);
//
//			if (id_p)
//				{
//					user_p = GetUserByNamedId (id_p, data_p -> usd_users_collection_s, MONGO_ID_S, data_p);
//
//					FreeBSONOid (id_p);
//				}
//			else
//				{
//					PrintErrors (STM_LEVEL_SEVERE, __FILE__, __LINE__, "Failed to create BSON OID for \"%s\"", user_id_s);
//				}
//
//		}		/* if (bson_oid_is_valid (field_trial_id_s, strlen (field_trial_id_s))) */
//	else
//		{
//			PrintErrors (STM_LEVEL_SEVERE, __FILE__, __LINE__, "\"%s\" is not a valid oid", user_id_s);
//		}
//
//	return user_p;
//}
//
//
//static User *GetUserByNamedId (const bson_oid_t *id_p, const char * const collection_s, const char *id_key_s, const UsersServiceData *data_p)
//{
//	User *user_p = NULL;
//	MongoTool *tool_p = data_p -> usd_mongo_p;
//
//	if (SetMongoToolCollection (tool_p, collection_s))
//		{
//			bson_t *query_p = bson_new ();
//			char id_s [MONGO_OID_STRING_BUFFER_SIZE];
//
//			bson_oid_to_string (id_p, id_s);
//
//			if (query_p)
//				{
//					if (BSON_APPEND_OID (query_p, id_key_s, id_p))
//						{
//							json_t *results_p = NULL;
//
//							#if DFW_UTIL_DEBUG >= STM_LEVEL_FINER
//								{
//									PrintBSONToLog (STM_LEVEL_FINER, __FILE__, __LINE__, query_p, "GetDFWObjectById query ");
//								}
//							#endif
//
//							results_p = GetAllMongoResultsAsJSON (tool_p, query_p, NULL);
//
//							if (results_p)
//								{
//									if (json_is_array (results_p))
//										{
//											size_t num_results = json_array_size (results_p);
//
//											if (num_results == 1)
//												{
//													json_t *res_p = json_array_get (results_p, 0);
//
//													user_p = GetUserFromJSON (res_p);
//
//													if (!user_p)
//														{
//															PrintJSONToErrors (STM_LEVEL_SEVERE, __FILE__, __LINE__, res_p, "failed to create User for id \"%s\"", id_s);
//														}
//
//												}		/* if (num_results == 1) */
//											else
//												{
//													PrintJSONToErrors (STM_LEVEL_SEVERE, __FILE__, __LINE__, results_p, SIZET_FMT " results when searching for object_id_s with id \"%s\"", num_results, id_s);
//												}
//
//										}		/* if (json_is_array (results_p) */
//									else
//										{
//											PrintJSONToErrors (STM_LEVEL_SEVERE, __FILE__, __LINE__, results_p, "Results are not an array");
//										}
//
//									json_decref (results_p);
//								}		/* if (results_p) */
//							else
//								{
//									PrintErrors (STM_LEVEL_SEVERE, __FILE__, __LINE__, "Failed to get results searching for object_id_s with id \"%s\"", id_s);
//								}
//
//						}		/* if (BSON_APPEND_OID (query_p, MONGO_ID_S, id_p)) */
//					else
//						{
//							PrintErrors (STM_LEVEL_SEVERE, __FILE__, __LINE__, "Failed to create query for object_id_s with id \"%s\"", id_s);
//						}
//
//					bson_destroy (query_p);
//				}		/* if (query_p) */
//			else
//				{
//					PrintErrors (STM_LEVEL_SEVERE, __FILE__, __LINE__, "Failed to create query for object_id_s with id \"%s\"", id_s);
//				}
//
//		}		/* if (SetMongoToolCollection (tool_p, data_p -> dftsd_collection_ss [collection_type])) */
//	else
//		{
//			PrintErrors (STM_LEVEL_SEVERE, __FILE__, __LINE__, "Failed to set collection to \"%s\"", collection_s);
//		}
//
//	return user_p;
//}



static bool SetUpDefaultsFromExistingUser (const User *user_p, char **id_ss)
{
	char *id_s = GetBSONOidAsString (user_p -> us_id_p);

	if (id_s)
		{
			*id_ss = id_s;

			return true;
		}		/* if (id_s) */
	else
		{
			PrintErrors (STM_LEVEL_SEVERE, __FILE__, __LINE__, "Failed to copy user id for \"%s\"", user_p -> us_email_s);
		}

	return false;

}


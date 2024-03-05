/*
 * groups_submission_service.c
 *
 *  Created on: 8 Dec 2023
 *      Author: billy
 */

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

#include "submission_service.h"
#include "users_service.h"

#include "audit.h"
#include "streams.h"
#include "math_utils.h"
#include "string_utils.h"
#include "schema_keys.h"

#include "json_parameter.h"

/*
 * Static declarations
 */

static const char * const S_ID_S = "id";

static NamedParameterType S_SET_DATA = { "Data", PT_JSON_TABLE };


static const char *GetGroupsSubmissionServiceName (const Service *service_p);

static const char *GetGroupsSubmissionServiceDescription (const Service *service_p);

static const char *GetGroupsSubmissionServiceAlias (const Service *service_p);

static const char *GetGroupsSubmissionServiceInformationUri (const Service *service_p);

static ParameterSet *GetGroupsSubmissionServiceParameters (Service *service_p, DataResource *resource_p, User *user_p);

static void ReleaseGroupsSubmissionServiceParameters (Service *service_p, ParameterSet *params_p);

static ServiceJobSet *RunGroupsSubmissionService (Service *service_p, ParameterSet *param_set_p, User *user_p, ProvidersStateTable *providers_p);

static ParameterSet *IsResourceForGroupsSubmissionService (Service *service_p, DataResource *resource_p, Handler *handler_p);

static bool CloseGroupsSubmissionService (Service *service_p);

static ServiceMetadata *GetGroupsSubmissionServiceMetadata (Service *service_p);

static bool GetGroupsSubmissionServiceParameterTypesForNamedParameters (const Service *service_p, const char *param_name_s, ParameterType *pt_p);


static bool AddChromosomes (json_t *doc_p, json_t *chromosomes_p);

static bool AddGeneticMappingPositions (json_t *doc_p, json_t *mappings_p);

static const char *AddParentRow (json_t *doc_p, json_t *genotypes_p, const char *key_s);

static bool AddGenotypesRow (json_t *doc_p, json_t *genotypes_p, UsersServiceData *data_p);

static bson_oid_t *SaveMarkers (const char **parent_a_ss, const char **parent_b_ss, const json_t *data_json_p, UsersServiceData *data_p);

static bool SaveVarieties (const char *parent_a_s, const char *parent_b_s, const bson_oid_t *id_p, UsersServiceData *data_p);

static bool SaveVariety (const char *parent_s, const bson_oid_t *id_p, MongoTool *mongo_p);

static char *GetAccession (const json_t *genotypes_p, UsersServiceData *data_p);


/*
 * API definitions
 */


Service *GetGroupsSubmissionService (GrassrootsServer *grassroots_p)
{
	Service *service_p = (Service *) AllocMemory (sizeof (Service));

	if (service_p)
		{
			UsersServiceData *data_p = AllocateUsersServiceData ();

			if (data_p)
				{
					if (InitialiseService (service_p,
																 GetGroupsSubmissionServiceName,
																 GetGroupsSubmissionServiceDescription,
																 GetGroupsSubmissionServiceAlias,
																 GetGroupsSubmissionServiceInformationUri,
																 RunGroupsSubmissionService,
																 IsResourceForGroupsSubmissionService,
																 GetGroupsSubmissionServiceParameters,
																 GetGroupsSubmissionServiceParameterTypesForNamedParameters,
																 ReleaseGroupsSubmissionServiceParameters,
																 CloseGroupsSubmissionService,
																 NULL,
																 false,
																 SY_SYNCHRONOUS,
																 (ServiceData *) data_p,
																 GetGroupsSubmissionServiceMetadata,
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



static const char *GetGroupsSubmissionServiceName (const Service * UNUSED_PARAM (service_p))
{
	return "Users submission service";
}


static const char *GetGroupsSubmissionServiceDescription (const Service * UNUSED_PARAM (service_p))
{
	return "A service to submit parental-cross genotype data";
}


static const char *GetGroupsSubmissionServiceAlias (const Service * UNUSED_PARAM (service_p))
{
	return GT_GROUP_ALIAS_PREFIX_S SERVICE_GROUP_ALIAS_SEPARATOR "submit";
}


static const char *GetGroupsSubmissionServiceInformationUri (const Service * UNUSED_PARAM (service_p))
{
	return NULL;
}


static ParameterSet *GetGroupsSubmissionServiceParameters (Service *service_p, DataResource * UNUSED_PARAM (resource_p), User * UNUSED_PARAM (user_p))
{
	ParameterSet *param_set_p = AllocateParameterSet ("Parental Genotype submission service parameters", "The parameters used for the Parental Genotype submission service");

	if (param_set_p)
		{
			ServiceData *data_p = service_p -> se_data_p;
			Parameter *param_p = NULL;
			ParameterGroup *group_p = CreateAndAddParameterGroupToParameterSet ("Parental Cross Data", false, data_p, param_set_p);

			if ((param_p = EasyCreateAndAddJSONParameterToParameterSet (data_p, param_set_p, group_p, S_SET_DATA.npt_type, S_SET_DATA.npt_name_s, "Data", "The parental-cross data", NULL, PL_SIMPLE)) != NULL)
				{
					if (AddParameterKeyStringValuePair (param_p, PA_TABLE_COLUMN_HEADERS_PLACEMENT_S, PA_TABLE_COLUMN_HEADERS_PLACEMENT_FIRST_ROW_S))
						{
							return param_set_p;
						}
				}
			else
				{
					PrintErrors (STM_LEVEL_SEVERE, __FILE__, __LINE__, "Failed to add %s parameter", S_SET_DATA.npt_name_s);
				}

			FreeParameterSet (param_set_p);
		}
	else
		{
			PrintErrors (STM_LEVEL_SEVERE, __FILE__, __LINE__, "Failed to allocate %s ParameterSet", GetGroupsSubmissionServiceName (service_p));
		}

	return NULL;
}


static bool GetGroupsSubmissionServiceParameterTypesForNamedParameters (const Service *service_p, const char *param_name_s, ParameterType *pt_p)
{
	bool success_flag = true;

	if (strcmp (param_name_s, S_SET_DATA.npt_name_s) == 0)
		{
			*pt_p = S_SET_DATA.npt_type;
		}
	else
		{
			success_flag = false;
		}

	return success_flag;
}


static void ReleaseGroupsSubmissionServiceParameters (Service * UNUSED_PARAM (service_p), ParameterSet *params_p)
{
	FreeParameterSet (params_p);
}




static bool CloseGroupsSubmissionService (Service *service_p)
{
	bool success_flag = true;

	FreeUsersServiceData ((UsersServiceData *) (service_p -> se_data_p));;

	return success_flag;
}



static ServiceJobSet *RunGroupsSubmissionService (Service *service_p, ParameterSet *param_set_p, User * UNUSED_PARAM (user_p), ProvidersStateTable * UNUSED_PARAM (providers_p))
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
					const json_t *data_json_p = NULL;

					if (GetCurrentJSONParameterValueFromParameterSet (param_set_p, S_SET_DATA.npt_name_s, &data_json_p))
						{
							status = OS_FAILED;

							if (data_json_p)
								{
									const char *parent_a_s = NULL;
									const char *parent_b_s = NULL;
									bson_oid_t *id_p = SaveMarkers (&parent_a_s, &parent_b_s, data_json_p, data_p);

									if (id_p)
										{
											if (SaveVarieties (parent_a_s, parent_b_s, id_p, data_p))
												{
													status = OS_SUCCEEDED;
												}

											FreeBSONOid (id_p);
										}		/* if (id_p) */

								}		/* if (data_json_p) */


						}		/* if (GetParameterValueFromParameterSet (param_set_p, S_SET_DATA.npt_name_s, &data_value, true)) */


				}		/* if (param_set_p) */

			SetServiceJobStatus (job_p, status);
			LogServiceJob (job_p);
		}		/* if (service_p -> se_jobs_p) */

	return service_p -> se_jobs_p;
}


static ServiceMetadata *GetGroupsSubmissionServiceMetadata (Service *service_p)
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


static bool AddChromosomes (json_t *doc_p, json_t *chromosomes_p)
{
	bool success_flag = true;
	void *iter_p = json_object_iter (chromosomes_p);

	while (iter_p && success_flag)
		{
			const char *key_s = json_object_iter_key (iter_p);

			if (strcmp (key_s, S_ID_S) != 0)
				{
					const char *value_s = GetJSONString (chromosomes_p, key_s);

					if (value_s)
						{
							bool added_flag = false;
							json_t *marker_p = json_object ();

							if (marker_p)
								{
									/*
									 * The marker name may contain full stops and although MongoDB 3.6+
									 * allows these, the current version of the mongo-c driver (1.13)
									 * does not, so we need to do the escaping ourselves
									 */
									char *escaped_marker_s = NULL;

									if (SearchAndReplaceInString (key_s, &escaped_marker_s, ".", PGS_ESCAPED_DOT_S))
										{
											if (json_object_set_new (doc_p, escaped_marker_s ? escaped_marker_s : key_s, marker_p)  == 0)
												{
													if (SetJSONString (marker_p, PGS_CHROMOSOME_S, value_s))
														{
															added_flag = true;
														}
												}

											if (escaped_marker_s)
												{
													FreeCopiedString (escaped_marker_s);
												}

										}		/* if (SearchAndReplaceInString (key_s, &escaped_marker_s, ".", PGS_DOT_S)) */

									if (!added_flag)
										{
											success_flag = false;
											json_decref (marker_p);
										}

								}		/* if (marker_p) */
							else
								{
									success_flag = false;
								}

						}		/* if (value_s) */
					else
						{
							success_flag = false;
						}

				}		/* if (strcmp (key_s, S_ID_S) != 0) */



			iter_p = json_object_iter_next (chromosomes_p, iter_p);
		}

	return success_flag;
}



static bool AddGeneticMappingPositions (json_t *doc_p, json_t *mappings_p)
{
	bool success_flag = true;
	void *iter_p = json_object_iter (mappings_p);

	while (iter_p && success_flag)
		{
			const char *key_s = json_object_iter_key (iter_p);

			if (strcmp (key_s, S_ID_S) != 0)
				{
					const char *value_s = GetJSONString (mappings_p, key_s);

					if (value_s)
						{
							/*
							 * The marker name may contain full stops and although MongoDB 3.6+
							 * allows these, the current version of the mongo-c driver (1.13)
							 * does not, so we need to do the escaping ourselves
							 */
							char *escaped_key_s = NULL;

							if (SearchAndReplaceInString (key_s, &escaped_key_s, ".", PGS_ESCAPED_DOT_S))
								{
									/* use key and value ... */
									json_t *marker_p = json_object_get (doc_p, escaped_key_s ? escaped_key_s : key_s);

									if (marker_p)
										{
											if (!SetJSONString (marker_p, PGS_MAPPING_POSITION_S, value_s))
												{
													PrintJSONToErrors (STM_LEVEL_SEVERE, __FILE__, __LINE__, marker_p, "Failed to set \"%s\": \"%s\"", PGS_MAPPING_POSITION_S, value_s);
													success_flag = false;
												}
										}		/* if (marker_p) */
									else
										{
											PrintJSONToErrors (STM_LEVEL_SEVERE, __FILE__, __LINE__, doc_p, "Failed to get \"%s\"", key_s);
											success_flag = false;
										}

									if (escaped_key_s)
										{
											FreeCopiedString (escaped_key_s);
										}

								}		/* if (SearchAndReplaceInString (key_s, &escaped_marker_s, ".", PGS_DOT_S)) */



						}		/* if (value_s) */
					else
						{
							PrintJSONToErrors (STM_LEVEL_SEVERE, __FILE__, __LINE__, mappings_p, "Failed to get value for \"%s\"", key_s);
							success_flag = false;
						}

				}		/* if (strcmp (key_s, S_ID_S) != 0) */

			iter_p = json_object_iter_next (mappings_p, iter_p);
		}

	return success_flag;
}


static const char *AddParentRow (json_t *doc_p, json_t *genotypes_p, const char *key_s)
{
	const char *parent_s = GetJSONString (genotypes_p, S_ID_S);

	if (parent_s)
		{
			if (SetJSONString (doc_p, key_s, parent_s))
				{
					return parent_s;
				}
		}

	return NULL;
}


/*
 * Paragon x Watkins 1190[0-9][0-9][0-9]" to "ParW[0-9][0-9][0-9]"
 */
static char *GetAccession (const json_t *genotypes_p, UsersServiceData *data_p)
{
	char *parents_s = NULL;
	const char *accession_s = GetJSONString (genotypes_p, S_ID_S);

	if (accession_s)
		{
			bool success_flag = true;

			if (data_p -> pgsd_name_mappings_p)
				{
					void *iterator_p = json_object_iter (data_p -> pgsd_name_mappings_p);

					while (iterator_p && success_flag)
						{
							const char *key_s = json_object_iter_key (iterator_p);
					    const size_t key_length = strlen (key_s);

							if (strncmp (accession_s, key_s, key_length) == 0)
								{
									json_t *value_p = json_object_iter_value (iterator_p);

									if (json_is_string (value_p))
										{
											const char *value_s = json_string_value (value_p);

											accession_s += key_length;
											parents_s = ConcatenateStrings (value_s, accession_s);

											if (!parents_s)
												{
													success_flag = false;
													PrintErrors (STM_LEVEL_SEVERE, __FILE__, __LINE__, "Failed to concatenate \"%s\" and \"%s\"", value_s, accession_s);
												}		/* if (!parents_s) */

										}		/* if (json_is_string (value_p)) */
									else
										{
											success_flag  = false;
											PrintJSONToErrors (STM_LEVEL_SEVERE, __FILE__, __LINE__, data_p -> pgsd_name_mappings_p, "Value for \"%s\" is not a string", key_s);
										}

								}		/* if (strncmp (accession_s, key_s, key_length) == 0) */

							if (success_flag)
								{
									iterator_p = json_object_iter_next (data_p -> pgsd_name_mappings_p, iterator_p);
								}

						}		/* while (iterator_p && success_flag) */

				}		/* if (data_p -> pgsd_name_mappings_p) */

			if (success_flag && (parents_s == NULL))
				{
					parents_s = EasyCopyToNewString (accession_s);

					if (!parents_s)
						{
							PrintErrors (STM_LEVEL_SEVERE, __FILE__, __LINE__, "Failed to copy \"%s\"", accession_s);
						}		/* if (!parents_s) */

				}

		}		/* if (accession_s) */

	return parents_s;
}


static bool AddGenotypesRow (json_t *doc_p, json_t *genotypes_p, UsersServiceData *data_p)
{
	bool success_flag = true;
	char *accession_s = GetAccession (genotypes_p, data_p);

	if (accession_s)
		{
			void *iter_p = json_object_iter (genotypes_p);

			while (iter_p && success_flag)
				{
					const char *key_s = json_object_iter_key (iter_p);

					if (strcmp (key_s, S_ID_S) != 0)
						{
							const char *value_s = GetJSONString (genotypes_p, key_s);

							if (value_s)
								{
									/*
									 * The marker name may contain full stops and although MongoDB 3.6+
									 * allows these, the current version of the mongo-c driver (1.13)
									 * does not, so we need to do the escaping ourselves
									 */
									char *escaped_key_s = NULL;

									if (SearchAndReplaceInString (key_s, &escaped_key_s, ".", PGS_ESCAPED_DOT_S))
										{
											/*
											 * use key and value ...
											 */
											json_t *marker_p = json_object_get (doc_p, escaped_key_s ? escaped_key_s : key_s);

											if (marker_p)
												{
													if (!SetJSONString (marker_p, accession_s, value_s))
														{
															PrintJSONToErrors (STM_LEVEL_SEVERE, __FILE__, __LINE__, marker_p, "Failed to set \"%s\": \"%s\"", accession_s, value_s);
															success_flag = false;
														}

												}		/* if (marker_p) */
											else
												{
													PrintJSONToErrors (STM_LEVEL_SEVERE, __FILE__, __LINE__, doc_p, "Failed to get marker for %s", key_s);
													success_flag = false;
												}

											if (escaped_key_s)
												{
													FreeCopiedString (escaped_key_s);
												}

										}		/* if (SearchAndReplaceInString (key_s, &escaped_marker_s, ".", PGS_DOT_S)) */

								}		/* if (value_s) */
							else
								{
									PrintJSONToErrors (STM_LEVEL_SEVERE, __FILE__, __LINE__, genotypes_p, "Failed to get %s", key_s);
									success_flag = false;
								}

						}		/* if (strcmp (key_s, S_ID_S) == 0) else ... */

					iter_p = json_object_iter_next (genotypes_p, iter_p);
				}

			FreeCopiedString (accession_s);
		}		/* if (accession_s) */
	else
		{
			PrintJSONToErrors (STM_LEVEL_SEVERE, __FILE__, __LINE__, genotypes_p, "Failed to get %s", S_ID_S);
			success_flag = false;
		}


	return success_flag;
}


static bson_oid_t *SaveMarkers (const char **parent_a_ss, const char **parent_b_ss, const json_t *data_json_p, UsersServiceData *data_p)
{
	bson_oid_t *id_p = NULL;
	bool success_flag = false;
	json_t *doc_p = json_object ();

	if (doc_p)
		{
			id_p = GetNewBSONOid ();

			if (id_p)
				{
					if (AddCompoundIdToJSON (doc_p, id_p))
						{
							/*
								The organisation is:
								1 row = marker name
								2 row = chromosome / linkage group name
								3 row = genetic mapping position
								4 row = Parent A (always Paragon for this set)
								5 row = Parent B (always a Watkins landrace accession in format "Watkins 1190[0-9][0-9][0-9]"
								6 to last row = individuals of that population, progenies from the cross of Parent A with Parent B

								We abbreviate the population names from correctly: "Paragon x Watkins 1190[0-9][0-9][0-9]" to "ParW[0-9][0-9][0-9]".
								The code 1190xxx was the original number these lines were stored in the germplasm resource unit.
							 */
							if (json_is_array (data_json_p))
								{
									const size_t num_rows = json_array_size (data_json_p);

									/*
									 * There are 2 header rows, so the actual genotype data doesn't
									 * start until row 3
									 */
									if (num_rows >= 3)
										{
											/*
											 * Since the first row, the marker names, is used as the headers, the first entry should be
											 * the chromosome / linkage group name
											 */
											size_t row_index = 0;
											json_t *row_p = json_array_get (data_json_p, row_index);

											if (AddChromosomes (doc_p, row_p))
												{
													/*
													 * genetic mapping position
													 */
													row_p = json_array_get (data_json_p, ++ row_index);

													if (AddGeneticMappingPositions (doc_p, row_p))
														{
															row_p = json_array_get (data_json_p, ++ row_index);
															const char *parent_a_s = AddParentRow (doc_p, row_p, PGS_PARENT_A_S);

															if (parent_a_s)
																{
																	row_p = json_array_get (data_json_p, ++ row_index);
																	const char *parent_b_s = AddParentRow (doc_p, row_p, PGS_PARENT_B_S);

																	if (parent_b_s)
																		{
																			char *name_s = ConcatenateVarargsStrings (parent_a_s, " x ", parent_b_s, NULL);

																			if (name_s)
																				{
																					if (SetJSONString (doc_p, PGS_POPULATION_NAME_S, name_s))
																						{
																							success_flag = true;

																							++ row_index;

																							while ((row_index < num_rows) && success_flag)
																								{
																									row_p = json_array_get (data_json_p, row_index);

																									if (AddGenotypesRow (doc_p, row_p, data_p))
																										{
																											++ row_index;
																										}
																									else
																										{
																											success_flag = false;
																										}

																								}		/* while ((row_index < num_rows) && success_flag) */

																							if (success_flag)
																								{
																									/*
																									 * Save the document
																									 */
																									bson_t *bson_doc_p = ConvertJSONToBSON (doc_p);

																									if (bson_doc_p)
																										{
																											/*
																											 * Is the doc ok to save in one go?
																											 */
																											if (bson_doc_p -> len < BSON_MAX_SIZE)
																												{
																													if (SaveMongoDataFromBSON (data_p -> pgsd_mongo_p, bson_doc_p, data_p -> pgsd_populations_collection_s, NULL))
																														{
																															*parent_a_ss = parent_a_s;
																															*parent_b_ss = parent_b_s;
																														}
																													else
																														{
																															success_flag = false;
																															PrintJSONToErrors (STM_LEVEL_SEVERE, __FILE__, __LINE__, doc_p, "Failed to save to \"%s\" -> \"%s\"", data_p -> pgsd_database_s, data_p -> pgsd_populations_collection_s);
																														}

																													bson_destroy (bson_doc_p);
																												}
																											else
																												{
																													/*
																													 * We need to break the doc up into more
																													 * manageable chunks so make a guess at how
																													 * many docs we need
																													 */
																													size_t num_docs = (size_t) ((double) (bson_doc_p -> len) / (double) BSON_MAX_SIZE) + 1;


																													/*
																													 * Free the memory of the large doc
																													 */
																													bson_destroy (bson_doc_p);

																												}


																										}		/* if (bson_doc_p) */


																								}

																						}		/* if (SetJSONString (doc_p, PGS_POPULATION_NAME_S, name_s)) */

																					FreeCopiedString (name_s);
																				}		/* if (name_s) */


																		}		/* if (parent_b_s) */

																}		/* if (parent_a_s) */

														}		/* if (AddGeneticMappingPositions (doc_p, row_p)) */

												}		/* if (AddChromosomes (doc_p, chromosomes_p)) */

										}		/* if (num_rows >= 3) */

								}		/* if (json_is_array (data_json_p)) */

						}		/* if (AddCompoundIdToJSON (doc_p, id_p)) */

				}		/* if (id_p) */


			json_decref (doc_p);
		}		/* if (doc_p) */

	if (!success_flag)
		{
			if (id_p)
				{
					FreeBSONOid (id_p);
				}
		}


	return success_flag ? id_p : NULL;
}


static bool SaveVarieties (const char *parent_a_s, const char *parent_b_s, const bson_oid_t *id_p, UsersServiceData *data_p)
{
	bool success_flag = false;
	MongoTool *tool_p = data_p -> pgsd_mongo_p;

	if (SetMongoToolCollection (tool_p, data_p -> pgsd_varieties_collection_s))
		{
			if (SaveVariety (parent_a_s, id_p, tool_p))
				{
					if (SaveVariety (parent_b_s, id_p, tool_p))
						{
							success_flag = true;
						}

				}

		}		/* if (SetMongoToolCollection (data_p -> pgsd_mongo_p, data_p -> pgsd_accessions_collection_s)) */

	return success_flag;
}


static bool SaveVariety (const char *parent_s, const bson_oid_t *id_p, MongoTool *mongo_p)
{
	bool success_flag = false;
	bson_t *query_p = bson_new ();

	if (query_p)
		{
			if (BSON_APPEND_UTF8 (query_p, PGS_POPULATION_NAME_S, parent_s))
				{
					bool done_flag = false;

					/*
					 * Is the accession already in the collection
					 */
					json_t *results_p = GetAllMongoResultsAsJSON (mongo_p, query_p, NULL);

					if (results_p)
						{
							if (json_is_array (results_p))
								{
									const size_t num_results = json_array_size (results_p);

									if (num_results == 1)
										{
											json_t *accession_data_p = json_array_get (results_p, 0);
											json_t *marker_ids_p = json_object_get (accession_data_p, PGS_VARIETY_IDS_S);

											if (marker_ids_p)
												{
													done_flag = true;

													if (json_is_array (marker_ids_p))
														{
															if (AddCompoundIdToJSONArray (marker_ids_p, id_p))
																{
																	if (SaveMongoData (mongo_p, accession_data_p, NULL, query_p))
																		{
																			success_flag = true;
																		}
																}
														}
												}
										}
								}

							json_decref (results_p);
						}		/* if (results_p) */


					/*
					 * Do we need to do an insert?
					 */
					if (! (success_flag && done_flag))
						{
							json_t *accession_data_p = json_object ();

							if (accession_data_p)
								{
									if (SetJSONString (accession_data_p, PGS_POPULATION_NAME_S, parent_s))
										{
											json_t *marker_ids_p = json_array ();

											if (marker_ids_p)
												{
													if (json_object_set_new (accession_data_p, PGS_VARIETY_IDS_S, marker_ids_p) == 0)
														{
															if (AddCompoundIdToJSONArray (marker_ids_p, id_p))
																{
																	if (SaveMongoData (mongo_p, accession_data_p, NULL, NULL))
																		{
																			success_flag = true;
																		}
																}
														}		/* if (json_object_set_new (accession_data_p, PGS_MARKER_IDS_S, marker_ids_p) == 0) */
													else
														{
															json_decref (marker_ids_p);
														}

												}		/* if (marker_ids_p) */

										}		/* if (SetJSONString (accession_data_p, PGS_POPULATION_NAME_S, parent_s)) */

									json_decref (accession_data_p);
								}		/* if (accession_data_p) */

						}		/* if (! (success_flag && done_flag)) */

				}		/* if (BSON_APPEND_UTF8 (query_p, PGS_POPULATION_NAME_S, parent_s)) */


			bson_destroy (query_p);
		}

	return success_flag;
}


static ParameterSet *IsResourceForGroupsSubmissionService (Service * UNUSED_PARAM (service_p), DataResource * UNUSED_PARAM (resource_p), Handler * UNUSED_PARAM (handler_p))
{
	return NULL;
}




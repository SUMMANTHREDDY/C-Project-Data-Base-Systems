/************************************************************
	Project#1:	CLP & DDL
 ************************************************************/

#include "db.h"
#include <stdio.h>
#include <string.h>
#include <stdlib.h>
#include <ctype.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <sstream>
#include <bits/stdc++.h>

using namespace std;

#if defined(_WIN32) || defined(_WIN64)
#define strcasecmp _stricmp
#endif

int main(int argc, char** argv)
{
	int rc = 0;
	token_list *tok_list=NULL, *tok_ptr=NULL, *tmp_tok_ptr=NULL;

//    *argv = "create table class ( Student char(12) NOT NULL ,Gender char(1),Exams int, Quiz int,Total int)";

    if ((argc != 2) || (strlen(argv[1]) == 0))
	{
		printf("Usage: db \"command statement\"\n");
		return 1;
	}

	rc = initialize_tpd_list();

  	if (rc)
  	{
		printf("\nError in initialize_tpd_list().\nrc = %d\n", rc);
  	}
	else
	{
    	rc = get_token(argv[1], &tok_list);

		/* Test code */
		tok_ptr = tok_list;
		while (tok_ptr != NULL)
		{
			// printf("%16s \t%d \t %d\n",tok_ptr->tok_string, tok_ptr->tok_class,
			// 	      tok_ptr->tok_value);
			tok_ptr = tok_ptr->next;
		}
    
		if (!rc)
		{
			printf("\n");
			rc = do_semantic(tok_list);
		}

		if (rc)
		{
			tok_ptr = tok_list;
			while (tok_ptr != NULL)
			{
				if ((tok_ptr->tok_class == error) ||
					  (tok_ptr->tok_value == INVALID))
				{

					printf("\n");
					//rc error print handling
					if (rc == INVALID_STATEMENT)
					{
						printf("Error: Statement is not valid. Invalid Syntax at %s\n",
						 tok_ptr->tok_string);
					}
					else if (rc == DATATYPE_MISMATCH_ERROR)
					{
						printf("Error: Datatype Mismatch. Invalid datatype: %s\n", 
						tok_ptr->tok_string);
					}
					else if (rc == COLUMN_NOT_EXIST)
					{
						printf("Error: Column %s does not exist\n", 
						tok_ptr->tok_string);

					}
					else if (rc == TABLE_NOT_EXIST)
					{
						printf("Error: Table %s does not exist\n", 
						tok_ptr->tok_string);
					}
					else if (rc == INPUT_DATA_TYPE_NOT_NULL_ERROR)
					{
						printf("Error: Input Data Type cannot be NULL\n");
					}
					else if (rc == INVALID_RELATIONAL_OPERATOR)
					{
						printf("Error: Invalid relational operator: %s\n", 
						tok_ptr->tok_string);
					}
					else if (rc == INVALID_AGGRE_FUNC_PARAM)
					{
						printf("Error: Invalid aggregate function parameter: %s\n", 
						tok_ptr->tok_string);
					}
					else if (rc == INVALID_DATATYPE)
					{
						printf("Error: Invalid datatype: %s\n", 
						tok_ptr->tok_string);
					}
					
					printf("Error in the string: %s\n", tok_ptr->tok_string);
					printf("rc=%d\n", rc);

					break;
				}
				tok_ptr = tok_ptr->next;
			}
		}

    /* Whether the token list is valid or not, we need to free the memory */
		tok_ptr = tok_list;
		while (tok_ptr != NULL)
		{
      tmp_tok_ptr = tok_ptr->next;
      free(tok_ptr);
      tok_ptr=tmp_tok_ptr;
		}
	}

	return rc;
}

/************************************************************* 
	This is a lexical analyzer for simple SQL statements
 *************************************************************/
int get_token(char* command, token_list** tok_list)
{
	int rc=0,i,j;
	char *start, *cur, temp_string[MAX_TOK_LEN];
	bool done = false;
	
	start = cur = command;
	while (!done)
	{
		bool found_keyword = false;

		/* This is the TOP Level for each token */
	  memset ((void*)temp_string, '\0', MAX_TOK_LEN);
		i = 0;

		/* Get rid of all the leading blanks */
		while (*cur == ' ')
			cur++;

		if (cur && isalpha(*cur))
		{
			// find valid identifier
			int t_class;
			do 
			{
				temp_string[i++] = *cur++;
			}
			while ((isalnum(*cur)) || (*cur == '_'));

			if (!(strchr(STRING_BREAK, *cur)))
			{
				/* If the next char following the keyword or identifier
				   is not a blank, (, ), or a comma, then append this
					 character to temp_string, and flag this as an error */
				temp_string[i++] = *cur++;
				add_to_list(tok_list, temp_string, error, INVALID);
				rc = INVALID;
				done = true;
			}
			else
			{

				// We have an identifier with at least 1 character
				// Now check if this ident is a keyword
				for (j = 0, found_keyword = false; j < TOTAL_KEYWORDS_PLUS_TYPE_NAMES; j++)
				{
					if ((strcasecmp(keyword_table[j], temp_string) == 0))
					{
						found_keyword = true;
						break;
					}
				}

				if (found_keyword)
				{
				  if (KEYWORD_OFFSET+j < K_CREATE)
						t_class = type_name;
					else if (KEYWORD_OFFSET+j >= F_SUM)
            t_class = function_name;
          else
					  t_class = keyword;

					add_to_list(tok_list, temp_string, t_class, KEYWORD_OFFSET+j);
				}
				else
				{
					if (strlen(temp_string) <= MAX_IDENT_LEN)
					  add_to_list(tok_list, temp_string, identifier, IDENT);
					else
					{
						add_to_list(tok_list, temp_string, error, INVALID);
						rc = INVALID;
						done = true;
					}
				}

				if (!*cur)
				{
					add_to_list(tok_list, "", terminator, EOC);
					done = true;
				}
			}
		}
		else if (isdigit(*cur))
		{
			// find valid number
			do 
			{
				temp_string[i++] = *cur++;
			}
			while (isdigit(*cur));

			if (!(strchr(NUMBER_BREAK, *cur)))
			{
				/* If the next char following the keyword or identifier
				   is not a blank or a ), then append this
					 character to temp_string, and flag this as an error */
				temp_string[i++] = *cur++;
				add_to_list(tok_list, temp_string, error, INVALID);
				rc = INVALID;
				done = true;
			}
			else
			{
				add_to_list(tok_list, temp_string, constant, INT_LITERAL);

				if (!*cur)
				{
					add_to_list(tok_list, "", terminator, EOC);
					done = true;
				}
			}
		}
		else if ((*cur == '(') || (*cur == ')') || (*cur == ',') || (*cur == '*')
		         || (*cur == '=') || (*cur == '<') || (*cur == '>'))
		{
			/* Catch all the symbols here. Note: no look ahead here. */
			int t_value;
			switch (*cur)
			{
				case '(' : t_value = S_LEFT_PAREN; break;
				case ')' : t_value = S_RIGHT_PAREN; break;
				case ',' : t_value = S_COMMA; break;
				case '*' : t_value = S_STAR; break;
				case '=' : t_value = S_EQUAL; break;
				case '<' : t_value = S_LESS; break;
				case '>' : t_value = S_GREATER; break;
			}

			temp_string[i++] = *cur++;

			add_to_list(tok_list, temp_string, symbol, t_value);

			if (!*cur)
			{
				add_to_list(tok_list, "", terminator, EOC);
				done = true;
			}
		}
    else if (*cur == '\'')
    {
      /* Find STRING_LITERRAL */
			int t_class;
      cur++;
			do 
			{
				temp_string[i++] = *cur++;
			}
			while ((*cur) && (*cur != '\''));

      temp_string[i] = '\0';

			if (!*cur)
			{
				/* If we reach the end of line */
				add_to_list(tok_list, temp_string, error, INVALID);
				rc = INVALID;
				done = true;
			}
      else /* must be a ' */
      {
        add_to_list(tok_list, temp_string, constant, STRING_LITERAL);
        cur++;
				if (!*cur)
				{
					add_to_list(tok_list, "", terminator, EOC);
					done = true;
        }
      }
    }
		else
		{
			if (!*cur)
			{
				add_to_list(tok_list, "", terminator, EOC);
				done = true;
			}
			else
			{
				/* not a ident, number, or valid symbol */
				temp_string[i++] = *cur++;
				add_to_list(tok_list, temp_string, error, INVALID);
				rc = INVALID;
				done = true;
			}
		}
	}
			
  return rc;
}

void add_to_list(token_list **tok_list, char *tmp, int t_class, int t_value)
{
	token_list *cur = *tok_list;
	token_list *ptr = NULL;

	// printf("%16s \t%d \t %d\n",tmp, t_class, t_value);

	ptr = (token_list*)calloc(1, sizeof(token_list));
	strcpy(ptr->tok_string, tmp);
	ptr->tok_class = t_class;
	ptr->tok_value = t_value;
	ptr->next = NULL;

  if (cur == NULL)
		*tok_list = ptr;
	else
	{
		while (cur->next != NULL)
			cur = cur->next;

		cur->next = ptr;
	}
	return;
}

int do_semantic(token_list *tok_list)
{
	int rc = 0, cur_cmd = INVALID_STATEMENT;
	bool unique = false;
  	token_list *cur = tok_list;

	if ((cur->tok_value == K_CREATE) &&
			((cur->next != NULL) && (cur->next->tok_value == K_TABLE)))
	{
		printf("CREATE TABLE statement\n");
		cur_cmd = CREATE_TABLE;
		cur = cur->next->next;
	}
	else if ((cur->tok_value == K_DROP) &&
					((cur->next != NULL) && (cur->next->tok_value == K_TABLE)))
	{
		printf("DROP TABLE statement\n");
		cur_cmd = DROP_TABLE;
		cur = cur->next->next;
	}
	else if ((cur->tok_value == K_LIST) &&
					((cur->next != NULL) && (cur->next->tok_value == K_TABLE)))
	{
		printf("LIST TABLE statement\n");
		cur_cmd = LIST_TABLE;
		cur = cur->next->next;
	}
	else if ((cur->tok_value == K_LIST) &&
					((cur->next != NULL) && (cur->next->tok_value == K_SCHEMA)))
	{
		printf("LIST SCHEMA statement\n");
		cur_cmd = LIST_SCHEMA;
		cur = cur->next->next;
	}
   	else if ((cur->tok_value == K_INSERT) &&
        ((cur->next != NULL) && (cur->next->tok_value == K_INTO)))
    {
        printf("INSERT statement\n");
        cur_cmd = INSERT;
        cur = cur->next->next;
    }
	else if (cur->tok_value == K_SELECT)
	{
		printf("SELECT statement\n");
		cur_cmd = SELECT;
		cur = cur->next;
	}
	else if (cur->tok_value == K_DELETE)
	{
		printf("DELETE statement\n");
		cur_cmd = DELETE;
		cur = cur->next;
	}
	else if (cur->tok_value == K_UPDATE)
	{
		printf("UPDATE statement\n");
		cur_cmd = UPDATE;
		cur = cur->next;
	}
	else
  	{
		printf("Invalid statement\n");
		rc = cur_cmd;
	}

	if (cur_cmd != INVALID_STATEMENT)
	{
		switch(cur_cmd)
		{
			case CREATE_TABLE:
						rc = sem_create_table(cur);
						break;
			case DROP_TABLE:
						rc = sem_drop_table(cur);
						break;
			case LIST_TABLE:
						rc = sem_list_tables();
						break;
			case LIST_SCHEMA:
						rc = sem_list_schema(cur);
						break;
            case INSERT:
                        rc = sem_insert(cur);
                        break;
            case SELECT:
                       rc = sem_select(cur);
                       break;
			case DELETE:
					   rc = sem_delete(cur);
					   break;
			case UPDATE:
					   rc = sem_update(cur);
					   break;

			default:
					; /* no action */
		}
	}
	
	return rc;
}

int sem_create_table(token_list *t_list)
{
	int rc = 0;
	token_list *cur;
	tpd_entry tab_entry;
	tpd_entry *new_entry = NULL;
	bool column_done = false;
	int cur_id = 0;
	cd_entry	col_entry[MAX_NUM_COL];


	memset(&tab_entry, '\0', sizeof(tpd_entry));
	cur = t_list;
	if ((cur->tok_class != keyword) &&
		  (cur->tok_class != identifier) &&
			(cur->tok_class != type_name))
	{
		// Error
		rc = INVALID_TABLE_NAME;
		cur->tok_value = INVALID;
	}
	else
	{
		if ((new_entry = get_tpd_from_list(cur->tok_string)) != NULL)
		{
			rc = DUPLICATE_TABLE_NAME;
			cur->tok_value = INVALID;
		}
		else
		{
			strcpy(tab_entry.table_name, cur->tok_string);
			cur = cur->next;
			if (cur->tok_value != S_LEFT_PAREN)
			{
				//Error
				rc = INVALID_TABLE_DEFINITION;
				cur->tok_value = INVALID;
			}
			else
			{
				memset(&col_entry, '\0', (MAX_NUM_COL * sizeof(cd_entry)));

				/* Now build a set of column entries */
				cur = cur->next;
				do
				{
					if ((cur->tok_class != keyword) &&
							(cur->tok_class != identifier) &&
							(cur->tok_class != type_name))
					{
						// Error
						rc = INVALID_COLUMN_NAME;
						cur->tok_value = INVALID;
					}
					else
					{
						int i;
						for(i = 0; i < cur_id; i++)
						{
              /* make column name case sensitive */
							if (strcmp(col_entry[i].col_name, cur->tok_string)==0)
							{
								rc = DUPLICATE_COLUMN_NAME;
								cur->tok_value = INVALID;
								break;
							}
						}

						if (!rc)
						{
							strcpy(col_entry[cur_id].col_name, cur->tok_string);
							col_entry[cur_id].col_id = cur_id;
							col_entry[cur_id].not_null = false;    /* set default */

							cur = cur->next;
							if (cur->tok_class != type_name)
							{
								// Error
								rc = INVALID_TYPE_NAME;
								cur->tok_value = INVALID;
							}
							else
							{
                /* Set the column type here, int or char */
								col_entry[cur_id].col_type = cur->tok_value;
								cur = cur->next;

								if (col_entry[cur_id].col_type == T_INT)
								{
									if ((cur->tok_value != S_COMMA) &&
										  (cur->tok_value != K_NOT) &&
										  (cur->tok_value != S_RIGHT_PAREN))
									{
										rc = INVALID_COLUMN_DEFINITION;
										cur->tok_value = INVALID;
									}
								  else
									{
										col_entry[cur_id].col_len = sizeof(int);

										if ((cur->tok_value == K_NOT) &&
											  (cur->next->tok_value != K_NULL))
										{
											rc = INVALID_COLUMN_DEFINITION;
											cur->tok_value = INVALID;
										}
										else if ((cur->tok_value == K_NOT) &&
											    (cur->next->tok_value == K_NULL))
										{
											col_entry[cur_id].not_null = true;
											cur = cur->next->next;
										}

										if (!rc)
										{
											/* I must have either a comma or right paren */
											if ((cur->tok_value != S_RIGHT_PAREN) &&
												  (cur->tok_value != S_COMMA))
											{
												rc = INVALID_COLUMN_DEFINITION;
												cur->tok_value = INVALID;
											}
											else
		                  					{
												if (cur->tok_value == S_RIGHT_PAREN)
												{
 													column_done = true;
												}
												cur = cur->next;
											}
										}
									}
								}   // end of T_INT processing
								else
								{
									// It must be char() or varchar()
									if (cur->tok_value != S_LEFT_PAREN)
									{
										rc = INVALID_COLUMN_DEFINITION;
										cur->tok_value = INVALID;
									}
									else
									{
										/* Enter char(n) processing */
										cur = cur->next;

										if (cur->tok_value != INT_LITERAL)
										{
											rc = INVALID_COLUMN_LENGTH;
											cur->tok_value = INVALID;
										}
										else
										{
											/* Got a valid integer - convert */
											col_entry[cur_id].col_len = atoi(cur->tok_string);
											cur = cur->next;

											if (cur->tok_value != S_RIGHT_PAREN)
											{
												rc = INVALID_COLUMN_DEFINITION;
												cur->tok_value = INVALID;
											}
											else
											{
												cur = cur->next;

												if ((cur->tok_value != S_COMMA) &&
														(cur->tok_value != K_NOT) &&
														(cur->tok_value != S_RIGHT_PAREN))
												{
													rc = INVALID_COLUMN_DEFINITION;
													cur->tok_value = INVALID;
												}
												else
												{
													if ((cur->tok_value == K_NOT) &&
														  (cur->next->tok_value != K_NULL))
													{
														rc = INVALID_COLUMN_DEFINITION;
														cur->tok_value = INVALID;
													}
													else if ((cur->tok_value == K_NOT) &&
																	 (cur->next->tok_value == K_NULL))
													{
														col_entry[cur_id].not_null = true;
														cur = cur->next->next;
													}

													if (!rc)
													{
														/* I must have either a comma or right paren */
														if ((cur->tok_value != S_RIGHT_PAREN) &&															  (cur->tok_value != S_COMMA))
														{
															rc = INVALID_COLUMN_DEFINITION;
															cur->tok_value = INVALID;
														}
														else
													  {
															if (cur->tok_value == S_RIGHT_PAREN)
															{
																column_done = true;
															}
															cur = cur->next;
														}
													}
												}
											}
										}	/* end char(n) processing */
									}
								} /* end char processing */
							}
						}  // duplicate column name
					} // invalid column name

					/* If rc=0, then get ready for the next column */
					if (!rc)
					{
						cur_id++;
					}

				} while ((rc == 0) && (!column_done));

				if ((column_done) && (cur->tok_value != EOC))
				{
					rc = INVALID_TABLE_DEFINITION;
					cur->tok_value = INVALID;
				}

				if (!rc)
				{
					/* Now finished building tpd and add it to the tpd list */
					tab_entry.num_columns = cur_id;
					tab_entry.tpd_size = sizeof(tpd_entry) + sizeof(cd_entry) *	tab_entry.num_columns;
				  	tab_entry.cd_offset = sizeof(tpd_entry);
					new_entry = (tpd_entry*)calloc(1, tab_entry.tpd_size);

					if (new_entry == NULL)
					{
						rc = MEMORY_ERROR;
					}
					else
					{
						memcpy((void*)new_entry,
							     (void*)&tab_entry,
									 sizeof(tpd_entry));

						memcpy((void*)((char*)new_entry + sizeof(tpd_entry)),
									 (void*)col_entry,
									 sizeof(cd_entry) * tab_entry.num_columns);

						rc = add_tpd_to_list(new_entry);

						free(new_entry);
					}
				}
			}
		}
	}

    if (rc == 0) {
        rc = create_table_file(tab_entry, col_entry);
    }
  return rc;
}

int create_table_file(tpd_entry tab_entry, cd_entry col_entry[]) {
    int rc = 0;
    FILE *fhandle = NULL;
    struct stat file_stat;
    char* file_name = strcat(tab_entry.table_name, ".tab");

    if ((fhandle = fopen(file_name, "rbc")) == NULL)
    {
        if((fhandle = fopen(file_name, "wbc")) == NULL)
        {
            rc = FILE_OPEN_ERROR;
            return rc;
        }
    }

    table_file_header header;
    memset(&header, '\0', sizeof(table_file_header));

    header.file_size = sizeof(table_file_header);
    header.record_size = 0;
    header.num_records = 0;
    header.record_offset = sizeof(table_file_header);
    header.file_header_flag = 1;
	header.tpd_ptr = 0;

    /* First, write the tpd_entry information */
    // printf("Table Name               (table_name)  = %s\n", tab_entry.table_name);
    // printf("Number of Columns        (num_columns) = %d\n", tab_entry.num_columns);

    for(int i = 0; i < tab_entry.num_columns; i++)
    {
        header.record_size += 1 + col_entry[i].col_len;
        // printf("Column Name   (col_name) = %s\n", col_entry[i].col_name);
        // printf("Column Type   (col_type) = %d\n", col_entry[i].col_type);
        // printf("Column Length (col_len)  = %d\n", col_entry[i].col_len);
    }

	int rem = header.record_size % 4;
	// printf("rec size %d\n", header.record_size);

	if (rem != 0) {
		int pad_num = 4 - rem;
		header.record_size += pad_num;
	}
	
    fwrite(&header, sizeof(table_file_header), 1, fhandle);

	printf("Created table successfully.\n\n");

	printf("Table File Header:\n");
	printf(" file_size: %d\n", header.file_size);
	printf(" record_offset: %d\n", header.record_offset);
	printf(" num_records: %d\n", header.num_records);
	printf(" record_size: %d\n\n", header.record_size);

    fflush(fhandle);
    fclose(fhandle);

    return rc;
}

int sem_drop_table(token_list *t_list)
{
	int rc = 0;
	token_list *cur;
	tpd_entry *tab_entry = NULL;

	cur = t_list;
	if ((cur->tok_class != keyword) &&
		  (cur->tok_class != identifier) &&
			(cur->tok_class != type_name))
	{
		// Error
		rc = INVALID_TABLE_NAME;
		cur->tok_value = INVALID;
	}
	else
	{
		if (cur->next->tok_value != EOC)
		{
			rc = INVALID_STATEMENT;
			cur->next->tok_value = INVALID;
		}
		else
		{
			if ((tab_entry = get_tpd_from_list(cur->tok_string)) == NULL)
			{
				rc = TABLE_NOT_EXIST;
				cur->tok_value = INVALID;
			}
			else
			{
				/* Found a valid tpd, drop it from tpd list */
				rc = drop_tpd_from_list(cur->tok_string);
				if (rc)
					return rc;

				// also delete respective .tab file
				string tab_name = string(cur->tok_string);
				string table_file_name = tab_name + ".tab";

				remove(table_file_name.c_str());

			}
		}
	}

  return rc;
}

int sem_list_tables()
{
	int rc = 0;
	int num_tables = g_tpd_list->num_tables;
	tpd_entry *cur = &(g_tpd_list->tpd_start);

	if (num_tables == 0)
	{
		printf("\nThere are currently no tables defined\n");
	}
	else
	{
		printf("\nTable List\n");
		printf("*****************\n");
		while (num_tables-- > 0)
		{
			printf("%s\n", cur->table_name);
			if (num_tables > 0)
			{
				cur = (tpd_entry*)((char*)cur + cur->tpd_size);
			}
		}
		printf("****** End ******\n");
	}

  return rc;
}

int sem_list_schema(token_list *t_list)
{
	int rc = 0;
	token_list *cur;
	tpd_entry *tab_entry = NULL;
	cd_entry  *col_entry = NULL;
	char tab_name[MAX_IDENT_LEN+1];
	char filename[MAX_IDENT_LEN+1];
	bool report = false;
	FILE *fhandle = NULL;
	int i = 0;

	cur = t_list;

	if (cur->tok_value != K_FOR)
  {
		rc = INVALID_STATEMENT;
		cur->tok_value = INVALID;
	}
	else
	{
		cur = cur->next;

		if ((cur->tok_class != keyword) &&
			  (cur->tok_class != identifier) &&
				(cur->tok_class != type_name))
		{
			// Error
			rc = INVALID_TABLE_NAME;
			cur->tok_value = INVALID;
		}
		else
		{
			memset(filename, '\0', MAX_IDENT_LEN+1);
			strcpy(tab_name, cur->tok_string);
			cur = cur->next;

			if (cur->tok_value != EOC)
			{
				if (cur->tok_value == K_TO)
				{
					cur = cur->next;
					
					if ((cur->tok_class != keyword) &&
						  (cur->tok_class != identifier) &&
							(cur->tok_class != type_name))
					{
						// Error
						rc = INVALID_REPORT_FILE_NAME;
						cur->tok_value = INVALID;
					}
					else
					{
						if (cur->next->tok_value != EOC)
						{
							rc = INVALID_STATEMENT;
							cur->next->tok_value = INVALID;
						}
						else
						{
							/* We have a valid file name */
							strcpy(filename, cur->tok_string);
							report = true;
						}
					}
				}
				else
				{ 
					/* Missing the TO keyword */
					rc = INVALID_STATEMENT;
					cur->tok_value = INVALID;
				}
			}

			if (!rc)
			{
				if ((tab_entry = get_tpd_from_list(tab_name)) == NULL)
				{
					rc = TABLE_NOT_EXIST;
					cur->tok_value = INVALID;
				}
				else
				{
					if (report)
					{
						if((fhandle = fopen(filename, "a+tc")) == NULL)
						{
							rc = FILE_OPEN_ERROR;
						}
					}

					if (!rc)
					{
						/* Find correct tpd, need to parse column and index information */

						/* First, write the tpd_entry information */
						printf("Table PD size            (tpd_size)    = %d\n", tab_entry->tpd_size);
						printf("Table Name               (table_name)  = %s\n", tab_entry->table_name);
						printf("Number of Columns        (num_columns) = %d\n", tab_entry->num_columns);
						printf("Column Descriptor Offset (cd_offset)   = %d\n", tab_entry->cd_offset);
            printf("Table PD Flags           (tpd_flags)   = %d\n\n", tab_entry->tpd_flags); 

						if (report)
						{
							fprintf(fhandle, "Table PD size            (tpd_size)    = %d\n", tab_entry->tpd_size);
							fprintf(fhandle, "Table Name               (table_name)  = %s\n", tab_entry->table_name);
							fprintf(fhandle, "Number of Columns        (num_columns) = %d\n", tab_entry->num_columns);
							fprintf(fhandle, "Column Descriptor Offset (cd_offset)   = %d\n", tab_entry->cd_offset);
              fprintf(fhandle, "Table PD Flags           (tpd_flags)   = %d\n\n", tab_entry->tpd_flags); 
						}

						/* Next, write the cd_entry information */
						for(i = 0, col_entry = (cd_entry*)((char*)tab_entry + tab_entry->cd_offset);
								i < tab_entry->num_columns; i++, col_entry++)
						{
							printf("Column Name   (col_name) = %s\n", col_entry->col_name);
							printf("Column Id     (col_id)   = %d\n", col_entry->col_id);
							printf("Column Type   (col_type) = %d\n", col_entry->col_type);
							printf("Column Length (col_len)  = %d\n", col_entry->col_len);
							printf("Not Null flag (not_null) = %d\n\n", col_entry->not_null);

							if (report)
							{
								fprintf(fhandle, "Column Name   (col_name) = %s\n", col_entry->col_name);
								fprintf(fhandle, "Column Id     (col_id)   = %d\n", col_entry->col_id);
								fprintf(fhandle, "Column Type   (col_type) = %d\n", col_entry->col_type);
								fprintf(fhandle, "Column Length (col_len)  = %d\n", col_entry->col_len);
								fprintf(fhandle, "Not Null Flag (not_null) = %d\n\n", col_entry->not_null);
							}
						}
	
						if (report)
						{
							fflush(fhandle);
							fclose(fhandle);
						}
					} // File open error							
				} // Table not exist
			} // no semantic errors
		} // Invalid table name
	} // Invalid statement

  return rc;
}

int sem_insert(token_list *t_list)
{
	int rc = 0;
	token_list *cur;
	tpd_entry *new_entry = NULL;
	cur = t_list;
	void *tab_file_data = NULL;
	char *rec_mem = NULL;
	
	// printf("cur string %s\n", cur->tok_string);
	// printf("cur string %d\n", cur->tok_class);
	// printf("cur string %d\n", cur->tok_value);

	// table_name token check
	if (cur->tok_class != identifier)
	{
		rc = INVALID_TABLE_NAME;
		cur->tok_value = INVALID;
		return rc;
	}
	else {
		// if table does not exist in the tpd list bail out
		if ((new_entry = get_tpd_from_list(cur->tok_string)) == NULL)
        {
            rc = TABLE_NOT_EXIST;
            cur->tok_value = INVALID;
			return rc;
        }
		else {

			// load corresponding .tab file into memory
			tab_file_data = get_table_file_data(new_entry->table_name);
			if (tab_file_data == NULL)
			{
				rc = MEMORY_ERROR;
				return rc;
			}

			table_file_header *tf_hdr = (table_file_header*)tab_file_data;
			// printf("tfh->file_size %d\n", tf_hdr->file_size);
			// printf("tfh->num_records %d\n", tf_hdr->num_records);
			// printf("tfh->record_offset %d\n", tf_hdr->record_offset);
			// printf("tfh->file_size %d\n", tf_hdr->record_size);

			// VALUES token check
			cur = cur->next;
			if ((cur->tok_class != keyword) || (cur->tok_value != K_VALUES))
			{
				rc = INVALID_STATEMENT;
				cur->tok_value = INVALID;
				return rc;
			}

			// ( paranthesis check
			cur = cur->next;
			if ((cur->tok_class != symbol) || (cur->tok_value != S_LEFT_PAREN))
			{
				rc = INVALID_STATEMENT;
				cur->tok_value = INVALID;
				return rc;
			}

			// get num of columns
			int num_cols = new_entry->num_columns;
			int rec_size = tf_hdr->record_size;
			cd_entry *col_arr = (cd_entry*)(((char *)new_entry) + new_entry->cd_offset); 

			rec_mem = (char *)calloc(1, sizeof(rec_size));
			if (!rec_mem) {
				rc = MEMORY_ERROR;
				return rc;
			}

			// safe checker TODO
			// free up malloc mem when failed

			// need to expect to parse the same num of literals next as the col number
			// loop through each one and parse them
			int idx = 0;
			int mem_offset = 0;

			for (idx = 0; idx < num_cols; idx++)
			{
				// this should be a constant or null
				cur = cur->next;
				if (cur->tok_class != constant && cur->tok_class != keyword) 
				{
					free(rec_mem);
					rc = INVALID_STATEMENT;
					cur->tok_value = INVALID;
					return rc;
				}

				/* save field size into rec mem - also set to zero if null*/

				// get the column size from cd entry

				int col_size = 0;

				if (cur->tok_value != K_NULL)
				{
					col_size = col_arr[idx].col_len;
				}

				char col_size_char = (char)col_size;

				// copy char byte into rec mem
				memcpy(rec_mem + mem_offset, &col_size_char, 1);
				// increment mem offset by 1 byte 
				mem_offset = mem_offset + 1;

				// the constant can either be a string or int literal
				if (cur->tok_value == STRING_LITERAL)
				{
					// check against the column type
					int v_col_type = col_arr[idx].col_type;
					if (v_col_type != T_CHAR && v_col_type != T_VARCHAR)
					{
						free(rec_mem);
						rc = DATATYPE_MISMATCH_ERROR;
						cur->tok_value = INVALID;
						return rc;
					}

					int str_length = strlen(cur->tok_string);
					if (str_length > col_size)
					{
						free(rec_mem);
						rc = INVALID_COLUMN_LENGTH;
						cur->tok_value = INVALID;
						return rc;
					} 

					// update len byte again
					mem_offset = mem_offset - 1;
					char new_char_size = (char)str_length;
					// copy char byte into rec mem
					memcpy(rec_mem + mem_offset, &new_char_size, 1);
					mem_offset = mem_offset + 1;

					// save the string literal value into memory
					memcpy(rec_mem + mem_offset, cur->tok_string, str_length);
					mem_offset = mem_offset + str_length;
					
				}
				else if (cur->tok_value == INT_LITERAL)
				{

					int v_col_type = col_arr[idx].col_type;
					if (v_col_type != T_INT)
					{
						free(rec_mem);
						rc = DATATYPE_MISMATCH_ERROR;
						cur->tok_value = INVALID;
						return rc;
					}
					
					int int_val = atoi(cur->tok_string);
					if (sizeof(int) > col_size)
					{
						free(rec_mem);
						rc = INVALID_COLUMN_LENGTH;
						cur->tok_value = INVALID;
						return rc;
					}

					// save the integer literal value into memory
					memcpy(rec_mem + mem_offset, &int_val, sizeof(int));
					mem_offset = mem_offset + sizeof(int);
				}
				else if (cur->tok_value == K_NULL)
				{
					int not_null_flag = col_arr[idx].not_null;

					// if not null flag is set bail out 
					if (not_null_flag)
					{
						free(rec_mem);
						rc = INPUT_DATA_TYPE_NOT_NULL_ERROR;
						cur->tok_value = INVALID;
						return rc;
					}
				}
				else
				{
					free(rec_mem);
					rc = INVALID_STATEMENT;
					cur->tok_value = INVALID;
					return rc;
				}

				// this should be a symbol : ')' for the last element and ',' for the rest
				cur = cur->next; 
				if (cur->tok_class != symbol)
				{
					free(rec_mem);
					rc = INVALID_STATEMENT;
					cur->tok_value = INVALID;
					return rc;
				}

				if (cur->tok_value == S_COMMA)
				{
					if (idx == num_cols-1)
					{
						free(rec_mem);
						rc = INVALID_STATEMENT;
						cur->tok_value = INVALID;
						return rc;
					}
					else
					{
						continue;
					}
				}
				else if (cur->tok_value == S_RIGHT_PAREN)
				{
					if (idx != num_cols - 1)
					{
						free(rec_mem);
						rc = INVALID_STATEMENT;
						cur->tok_value = INVALID;
						return rc;
					}
					else {
						continue;
					}
				}

			}

			// post this we should ideally be left with only EOC
			cur = cur->next;
			if (cur->tok_value != EOC)
			{
				free(rec_mem);
				rc = INVALID_STATEMENT;
				cur->tok_value = INVALID;
				return rc;
			}
			else {

				// save rec mem into tab file data
				rc = append_record_to_table_file(new_entry->table_name,
									 tab_file_data, rec_mem, rec_size);

			}

		}
	}

free_rec_mem:
	if (rec_mem != NULL)
		free(rec_mem);
free_table_file_data:
	if (tab_file_data != NULL)
		free(tab_file_data);
	return rc;
}

int append_record_to_table_file(char *table_name, void *tab_file_data,
	 void *rec_mem, int rec_size)
{

	int rc = 0;
	char file_table_name[MAX_IDENT_LEN+4];
	void *mem_ptr = NULL;
	FILE *fhandle = NULL;

	memset(file_table_name, 0, MAX_IDENT_LEN+4);
	strcpy(file_table_name, table_name);
	strcat(file_table_name, ".tab");

	// update table file header
	table_file_header *tf_hdr = (table_file_header*)tab_file_data;
	tf_hdr->num_records++;
	int old_file_size = tf_hdr->file_size;
	tf_hdr->file_size += rec_size;

	if((fhandle = fopen(file_table_name, "wbc")) == NULL) {
		rc = FILE_OPEN_ERROR;
		return rc;
	}
	else {
		fwrite((void *)tab_file_data, old_file_size, 1, fhandle);
		fflush(fhandle);
		fclose(fhandle);
	}

	// append insert record data at the end of the table file
	if((fhandle = fopen(file_table_name, "abc")) == NULL) {
		rc = FILE_OPEN_ERROR;
		return rc;
	}
	else {
		// printf("rec size %d\n", rec_size);
		
		fwrite(rec_mem, rec_size, 1, fhandle);
		fflush(fhandle);
		fclose(fhandle);
	}

	printf("Inserted row successfully.\n\n");

	printf("Table File Header (Latest):\n");
	printf(" file_size: %d\n", tf_hdr->file_size);
	printf(" record_offset: %d\n", tf_hdr->record_offset);
	printf(" num_records: %d\n", tf_hdr->num_records);
	printf(" record_size: %d\n\n", tf_hdr->record_size);

	return rc;
}

void* get_table_file_data(char *table_name)
{
	char file_table_name[MAX_IDENT_LEN+4];
	void *mem_ptr = NULL;
	FILE *fhandle = NULL;
	struct stat file_stat;

	memset(file_table_name, 0, MAX_IDENT_LEN+4);
	strcpy(file_table_name, table_name);
	strcat(file_table_name, ".tab");

	if((fhandle = fopen(file_table_name, "rbc")) == NULL) {
		return mem_ptr;
	}
	else {

		fstat(fileno(fhandle), &file_stat);
		mem_ptr = calloc(1, file_stat.st_size);
		if (mem_ptr == NULL)
		{
			return mem_ptr;
		}

		fread(mem_ptr, file_stat.st_size, 1, fhandle);

		fflush(fhandle);
		fclose(fhandle);
	}

	return mem_ptr;
} 

int sem_select(token_list *t_list)
{
	int rc = 0;
	token_list *cur;
	tpd_entry *new_entry = NULL;
	cur = t_list;
	vector<int> col_idx_list;
	int func_code = -1;
	token_list *save_col_tok;
	
	// printf("cur string %s\n", cur->tok_string);
	// printf("cur string %d\n", cur->tok_class);
	// printf("cur string %d\n", cur->tok_value);

	// if not * or a identifier bail out
	if (cur->tok_class != symbol && cur->tok_class != identifier && cur->tok_class != function_name)
	{
		rc = INVALID_STATEMENT;
		cur->tok_value = INVALID;
		return rc;
	}

	if (cur->tok_class == function_name)
	{
		func_code = cur->tok_value;

		cur = cur->next;
		// next token should be (
		if (cur->tok_value != S_LEFT_PAREN)
		{
			rc = INVALID_STATEMENT;
			cur->tok_value = INVALID;
			return rc;
		}
		cur = cur->next;
	}

	if (cur->tok_class == symbol)
	{
		if (cur->tok_value != S_STAR)
		{
			rc = INVALID_STATEMENT;
			cur->tok_value = INVALID;
			return rc;
		}

		// if function code is SUM/AVG then bail out as it is SUM(*)/AVG(*)
		if (func_code == F_SUM || func_code == F_AVG)
		{
			rc = INVALID_AGGRE_FUNC_PARAM;
			cur->tok_value = INVALID;
			return rc;
		}

		if (func_code == S_COMMA)
		{
			rc = INVALID_AGGRE_FUNC_PARAM;
			cur->tok_value = INVALID;
			return rc;
		}

		// if function name given next should be a )
		if (func_code != -1)
		{
			cur = cur->next;
			if (cur->tok_value != S_RIGHT_PAREN)
			{
				rc = INVALID_STATEMENT;
				cur->tok_value = INVALID;
				return rc;
			}
		}

		// parse next 2 tokens - FROM table_name
		cur = cur->next;
		if (cur->tok_value != K_FROM)
		{
			rc = INVALID_STATEMENT;
			cur->tok_value = INVALID;
			return rc;
		}

		cur = cur->next;
		if (cur->tok_class != identifier)
		{
			rc = INVALID_STATEMENT;
			cur->tok_value = INVALID;
			return rc;
		}

		// get idx for all cols
		char *table_name = cur->tok_string;

		// if table does not exist in the tpd list bail out
		if ((new_entry = get_tpd_from_list(table_name)) == NULL)
        {
            rc = TABLE_NOT_EXIST;
            cur->tok_value = INVALID;
			return rc;
        }
		else {
			
			int idx = 0;

			for (idx = 0; idx < new_entry->num_columns; idx++)
			{
				col_idx_list.push_back(idx);
			}
		}
	}
	else {
		vector<token_list*> col_names;

		if (func_code == -1) {
			// parse all column names
			if (cur->tok_class != identifier)
			{
				rc = INVALID_STATEMENT;
				cur->tok_value = INVALID;
				return rc;
			}

			int next_ident = 1;

			while(cur->tok_class == identifier || cur->tok_class == symbol)
			{
				// printf("cur->string %s\n", cur->tok_string);

				if (cur->tok_class == identifier)
				{
					if (next_ident != 1)
					{
						rc = INVALID_STATEMENT;
						cur->tok_value = INVALID;
						return rc;
					}
					// store col name (need to validate later)
					col_names.push_back(cur);
					next_ident = 0;
				}
				else {
					// back to back 2 identifiers without comma
					if (next_ident != 0)
					{
						rc = INVALID_STATEMENT;
						cur->tok_value = INVALID;
						return rc;	
					}

					if (cur->tok_value != S_COMMA)
					{
						break;
					}

					next_ident = 1;
				}
				cur = cur->next;
			}
		}

		// if function name given next should be a )
		if (func_code != -1)
		{
			if (cur->tok_class != identifier)
			{
				rc = INVALID_STATEMENT;
				cur->tok_value = INVALID;
				return rc;
			}
			
			col_names.push_back(cur);
			save_col_tok = cur;

			cur = cur->next;

			if (cur->tok_value != S_RIGHT_PAREN)
			{
				rc = INVALID_STATEMENT;
				cur->tok_value = INVALID;
				return rc;
			}
			cur = cur->next;
		}

		// parse next token - need to be from
		if (cur->tok_value != K_FROM)
		{
			rc = INVALID_STATEMENT;
			cur->tok_value = INVALID;
			return rc;	
		}

		// parse next token - need to be table_name
		cur = cur->next;
		if (cur->tok_class != identifier)
		{
			rc = INVALID_STATEMENT;
			cur->tok_value = INVALID;
			return rc;
		}

		char *table_name = cur->tok_string;
		if ((new_entry = get_tpd_from_list(table_name)) == NULL)
        {
            rc = TABLE_NOT_EXIST;
            cur->tok_value = INVALID;
			return rc;
        }
		else {
			// get idx for the given column names (stored in vector list)

			// get col arr
			cd_entry *col_arr = (cd_entry*)(((char *)new_entry) + new_entry->cd_offset);

			int idx1 = 0;
			for (idx1 = 0; idx1 < col_names.size(); idx1++)
			{
				token_list *col_token = col_names[idx1];
				char *i_col_name = col_token->tok_string;

				// check if this col name is present in cd array
				int idx2 = 0;
				int not_exist = 1;
				for (idx2 = 0; idx2 < new_entry->num_columns; idx2++)
				{
					if (strcmp(col_arr[idx2].col_name,i_col_name) == 0)
					{
						not_exist = 0;

						if (func_code == F_AVG || func_code == F_SUM)
						{
							// col data type should only be T_INT
							int dt_col_type = col_arr[idx2].col_type;
							if (dt_col_type != T_INT)
							{
								rc = INVALID_AGGRE_FUNC_PARAM;
								cur = save_col_tok;
								cur->tok_value = INVALID;
								return rc;
							}
						}

						col_idx_list.push_back(idx2);
					}
				}

				// if column name not present in cd arr bail out
				if (not_exist)
				{
					cur = col_token;
					rc = COLUMN_NOT_EXIST;
					cur->tok_value = INVALID;
					return rc;
				}
			}
		}	
	}

	cur = cur->next;
	if (cur->tok_value == EOC)
	{
		vector<int> empty_l1;
		vector<char *> empty_l2;

		rc = display_select_cols(new_entry, col_idx_list, empty_l1, empty_l2,
		 empty_l2, empty_l1, func_code, -1, -1);
		return rc;
	}
	else {
		// either can be 'WHERE' or 'ORDER BY' i.e only keyword
		if (cur->tok_class != keyword)
		{
			rc = INVALID_STATEMENT;
			cur->tok_value = INVALID;
			return rc;
		}
		else {
			vector<int> col_list;
			vector<char*> val_rel_list;
			vector<char*> const_list;
			vector<int> cond_list; 

			if (cur->tok_value == K_WHERE)
			{

				cur = cur->next;
				if (cur->tok_class != identifier)
				{
					rc = INVALID_STATEMENT;
					cur->tok_value = INVALID;
					return rc;
				}

				int next_flag = 1;
				int cur_col_idx = -1;
				char *cur_sym = NULL;
				char *cur_const = NULL;

				while ((cur->tok_class == identifier) || (cur->tok_class == symbol)
				 || cur->tok_class == constant || cur->tok_class == keyword)
				{
					if (cur->tok_class == identifier)
					{
						if (next_flag != 1)
						{
							rc = INVALID_STATEMENT;
							cur->tok_value = INVALID;
							return rc;
						}

						// check if column exists
						cd_entry *col_arr = (cd_entry*)(((char *)new_entry) + new_entry->cd_offset);
						int k = 0;

						int f_col_idx = -1;
						for (k = 0; k < new_entry->num_columns; k++)
						{
							char *col_name = col_arr[k].col_name;

							if (strcmp(col_name, cur->tok_string) == 0)
							{
								f_col_idx = k;
							}
						}

						if (f_col_idx == -1)
						{
							rc = COLUMN_NOT_EXIST;
							cur->tok_value = INVALID;
							return rc;
						}
						
						cur_col_idx = f_col_idx;
						next_flag = 2;
						cur  = cur->next;
						continue;
					}
					else if (cur->tok_class == symbol)
					{
						if (next_flag != 2)
						{
							rc = INVALID_STATEMENT;
							cur->tok_value = INVALID;
							return rc;
						}

						if (cur->tok_value != S_GREATER && cur->tok_value != S_LESS && cur->tok_value != S_EQUAL)
						{
							rc = INVALID_RELATIONAL_OPERATOR;
							cur->tok_value = INVALID;
							return rc;
						}

						cur_sym = cur->tok_string;
						next_flag = 3;
						cur = cur->next;
						continue;
					}
					else if (cur->tok_class == constant)
					{
						if (next_flag != 3) 
						{
							rc = INVALID_STATEMENT;
							cur->tok_value = INVALID;
							return rc;
						}

						if (cur->tok_value == STRING_LITERAL || cur->tok_value == INT_LITERAL)
						{
							//char *cur_const = cur->tok_string;
							// printf("came into const save als well %s", cur->tok_string);
						}
						else {
							rc = INVALID_STATEMENT;
							cur->tok_value = INVALID;
							return rc;
						}

						// store cur vals
						// check for any datatype mismatches
						cd_entry *col_arr = (cd_entry*)(((char *)new_entry) + new_entry->cd_offset);
						int dt_col_type = col_arr[cur_col_idx].col_type;

						if (cur->tok_value == STRING_LITERAL)
						{
							if (dt_col_type != T_CHAR && dt_col_type != T_VARCHAR)
							{
								rc = DATATYPE_MISMATCH_ERROR;
								cur->tok_value = INVALID;
								return rc;
							}
							
						}
						else {
							if (dt_col_type != T_INT)
							{
								rc = DATATYPE_MISMATCH_ERROR;
								cur->tok_value = INVALID;
								return rc;
							}

						}

						val_rel_list.push_back(cur_sym);
						const_list.push_back(cur->tok_string);
						col_list.push_back(cur_col_idx);

						cur_col_idx = -1;
						cur_sym = NULL;
						cur_const = NULL;

						next_flag = 4;
						cur = cur->next;

					}
					else if (cur->tok_class == keyword)
					{
						if (next_flag != 4 && next_flag != 2)
						{
							rc = INVALID_STATEMENT;
							cur->tok_value = INVALID;
							return rc;
						}

						if (next_flag == 2)
						{
							// case for IS NULL , IS NOT NULL
							if (cur->tok_value != K_IS)
							{
								rc = INVALID_RELATIONAL_OPERATOR;
								cur->tok_value = INVALID;
								return rc;
							}

							cur = cur->next;

							if (cur->tok_value != K_NOT && cur->tok_value != K_NULL)
							{
								rc = INVALID_DATATYPE;
								cur->tok_value = INVALID;
								return rc;
							}

							if (cur->tok_value == K_NOT)
							{
								cur = cur->next;
								if (cur->tok_value != K_NULL)
								{
									rc = INVALID_DATATYPE;
									cur->tok_value = INVALID;
									return rc;
								}

								// is not null
								char *new_sym = "isnot";
								val_rel_list.push_back(new_sym);
								const_list.push_back(cur->tok_string);
							}
							else if (cur->tok_value == K_NULL)
							{
								// is null
								char *new_sym = "is";
								val_rel_list.push_back(new_sym);
								const_list.push_back(cur->tok_string);
							}
							
							// store cur vals
							col_list.push_back(cur_col_idx);

							cur_col_idx = -1;
							cur_sym = NULL;
							cur_const = NULL;

							next_flag = 4;
							cur = cur->next;
						}
						else if(next_flag == 4)
						{
							if (cur->tok_value != K_AND && cur->tok_value != K_OR)
							{
								break;
							}
							else {

								cond_list.push_back(cur->tok_value);
								next_flag = 1;

								cur = cur->next;
								continue;
							}
						}

					}

					
				}

			}

			// printf("\n");
			// get next tokens for conditions - these are optional (TODO)

			if (cur->tok_value == EOC)
			{
				rc = display_select_cols(new_entry, col_idx_list, col_list,
					val_rel_list, const_list, cond_list, func_code, -1, -1);
				return rc;
			}
			
			if (cur->tok_value != K_ORDER)
			{
				rc = INVALID_STATEMENT;
				cur->tok_value = INVALID;
				return rc;
			}

			cur = cur->next;

			if (cur->tok_value != K_BY)
			{
				rc = INVALID_STATEMENT;
				cur->tok_value = INVALID;
				return rc;
			}

			cur = cur->next;

			if (cur->tok_class != identifier)
			{
				rc = INVALID_STATEMENT;
				cur->tok_value = INVALID;
				return rc;
			}

			char *i_col_name = cur->tok_string;
			int o_col_idx = -1;
			int idx3 = 0;
			cd_entry *col_arr = (cd_entry*)(((char *)new_entry) + new_entry->cd_offset);

			for (idx3 = 0; idx3 < new_entry->num_columns; idx3++)
			{
				if (strcmp(col_arr[idx3].col_name, i_col_name) == 0)
				{
					o_col_idx = idx3;
					// col_idx_list.push_back(idx3);
				}
			}

			// if column name not present in cd arr bail out
			if (o_col_idx == -1)
			{
				rc = COLUMN_NOT_EXIST;
				cur->tok_value = INVALID;
				return rc;
			}

			cur = cur->next;

			if (cur->tok_value != EOC && cur->tok_value != K_DESC)
			{
				rc = INVALID_STATEMENT;
				cur->tok_value = INVALID;
				return rc;
			}

			int order = 0;

			if (cur->tok_value != EOC)
			{
				order = 1;
				// ascending order call here
			}

			rc = display_select_cols(new_entry, col_idx_list, col_list,
				val_rel_list, const_list, cond_list, func_code, o_col_idx, order);
			return rc;

		}

	}

	// int t_itr = 0;
	// for (t_itr = 0; t_itr < col_idx_list.size(); t_itr++)
	// {
	// 	printf("%d,",col_idx_list[t_itr]);
	// }
	return rc;
}

int display_select_cols(tpd_entry *new_entry, vector<int> col_idx_list,
 vector<int> wh_col_list, vector<char *>wh_sym_list, vector<char *>wh_const_list,
 vector<int> wh_cond_list, int func_code, int o_col_idx, int order)
{
	int itr = 0;
	cd_entry *col_arr = (cd_entry*)(((char *)new_entry) + new_entry->cd_offset);
	void *table_file_data = get_table_file_data(new_entry->table_name);

	// printf("func_code: %d\n", func_code);

	table_file_header* tf_hdr = (table_file_header*)table_file_data;
	int rec_offset = tf_hdr->record_offset;
	int rec_size = tf_hdr->record_size;
	int num_cols = new_entry->num_columns;
	char *rec_mem = ((char *)table_file_data + rec_offset);
	char *cur_ptr = rec_mem;
	char *end_ptr = ((char *)table_file_data + tf_hdr->file_size);

	vector<int> col_width_arr;
	// init col width arr with size = col name + 5

	int f_itr = 0;
	for (f_itr = 0; f_itr < num_cols; f_itr++)
	{
		char *f_col_name = col_arr[f_itr].col_name;
		int col_name_width = strlen(f_col_name);

		col_width_arr.push_back(col_name_width);
	}

	// load full table into memory
	vector< vector< void*> > full_table_data;
	for (itr = 0; itr < num_cols; itr++)
	{
		int col_idx = itr;

		vector<void *> col_data;
		char *cur_ptr = rec_mem;
		char *end_ptr = ((char *)table_file_data + tf_hdr->file_size);

		// iterate through all the records and get only those cols whose idx matches
		while (cur_ptr != end_ptr)
		{
			char *temp_ptr = cur_ptr;
			int idx = 0;
			for (idx = 0; idx <= col_idx; idx++)
			{
				// get size
				int field_size = (int)*(char *)temp_ptr;

				if (idx == col_idx) {
					// read and print this
					void *field_mem = calloc(1, field_size+1);
					memcpy(field_mem, temp_ptr, field_size+1);
					col_data.push_back(field_mem);

					//get col type
					int f_col_type = col_arr[idx].col_type;

					if (f_col_type == T_CHAR || f_col_type == T_VARCHAR)
					{
						char *str_size_char = (char*)field_mem;
						int str_size = (int)*str_size_char;

						if (str_size == 0)
						{
							int f_cur_wid = col_width_arr[idx];
							if (f_cur_wid < 4)
							{
								col_width_arr[idx] = 4;
							}
							continue;
						}
						else {
							int f_cur_wid = col_width_arr[idx];
							if (f_cur_wid < str_size)
							{
								col_width_arr[idx] = str_size;
							}	
						}
					}
					else
					{
						char *int_field_size_char = (char *)field_mem;
						int int_field_size = (int)*int_field_size_char;

						if (int_field_size == 0)
						{
							int f_cur_wid = col_width_arr[idx];
							if (f_cur_wid < 4)
							{
								col_width_arr[idx] = 4;
							}
							continue;
						}
						else
						{
							int *field_val = (int *)((char*)field_mem + 1);
							string f_int_str = to_string(*field_val);
							int int_len = f_int_str.length();

							int f_cur_wid = col_width_arr[idx];
							if (f_cur_wid < int_len)
							{
								col_width_arr[idx] = int_len;
							} 
							continue;
						}

					}

				}
				else {
					// skip this
					temp_ptr = temp_ptr + field_size + 1;
				}
			}	
			cur_ptr = cur_ptr +  rec_size;
		}

		full_table_data.push_back(col_data);
	}


	// selected column names table
	vector< vector< void*> > table_data;
	for (itr = 0; itr < col_idx_list.size(); itr++)
	{
		int col_idx = col_idx_list[itr];

		vector<void *> col_data;
		char *cur_ptr = rec_mem;
		char *end_ptr = ((char *)table_file_data + tf_hdr->file_size);

		// iterate through all the records and get only those cols whose idx matches
		while (cur_ptr != end_ptr)
		{
			char *temp_ptr = cur_ptr;
			int idx = 0;
			for (idx = 0; idx <= col_idx; idx++)
			{
				// get size
				int field_size = (int)*(char *)temp_ptr;

				if (idx == col_idx) {
					// read and print this
					void *field_mem = calloc(1, field_size+1);
					memcpy(field_mem, temp_ptr, field_size+1);
					col_data.push_back(field_mem);
				}
				else {
					// skip this
					temp_ptr = temp_ptr + field_size + 1;
				}
			}	
			cur_ptr = cur_ptr +  rec_size;
		}

		table_data.push_back(col_data);
	}

	int i = 0;
	int j = 0;
	int col_height = 0;

	for (i = 0; i < table_data.size(); i++)
	{
		col_height = table_data[i].size();
	}

	vector<int> fin_order;

	// order by asc or desc
	// printf("o_col_idx %d, order %d\n", o_col_idx, order);
	if (o_col_idx != -1)
	{
		// generate the iteration list and the display will go through the rows in that order
		vector<int> idx_order;
		vector<int> null_order;
		
		
		// get the column type
		int col_type = col_arr[o_col_idx].col_type;

		if (order  == 0)
		{
			if (col_type == T_CHAR || col_type == T_VARCHAR)
			{
				priority_queue<pair<string, int>, vector< pair<string, int> >, greater<pair<string, int> > > order_idx_pq;

				// iterate entire col and push the val and idx
				int c_itr = 0;
				vector<void *> o_col = full_table_data[o_col_idx];

				for (c_itr = 0; c_itr < o_col.size(); c_itr++)
				{
					char *str_size_char = (char*)o_col[c_itr];
					int str_size = (int)*str_size_char;

					if (str_size == 0)
					{
						null_order.push_back(c_itr);
						continue;
					}
					else
					{
						char *field_start_char = ((char *)o_col[c_itr]) + 1;
						char field_val[str_size];

						memset(&field_val, '\0', str_size);
						memcpy(&field_val, field_start_char, str_size);
						// field_val[str_size] = '\0';

						string in_str = string(field_val);

						order_idx_pq.push(make_pair(in_str, c_itr));
					}

				}

				// pop all the order_pq values and push into idx order
				while (!order_idx_pq.empty())
				{
					pair<string, int> top = order_idx_pq.top();
					idx_order.push_back(top.second);

					order_idx_pq.pop();
				}

			}
			else
			{
				priority_queue<pair<int, int>, vector< pair<int, int> >, greater<pair<int, int> > > order_idx_pq;

				// iterate entire col and push the val and idx
				int c_itr = 0;
				vector<void *> o_col = full_table_data[o_col_idx];

				for (c_itr = 0; c_itr < o_col.size(); c_itr++)
				{
					char *int_field_size_char = (char *)o_col[c_itr];
					int int_field_size = (int)*int_field_size_char;

					if (int_field_size == 0)
					{
						null_order.push_back(c_itr);
						continue;
					}
					else
					{
						int *field_val = (int *)((char*)o_col[c_itr] + 1);
						int in_int = *field_val;
						order_idx_pq.push(make_pair(in_int, c_itr));
					}

				}

				// pop all the order_pq values and push into idx order
				while (!order_idx_pq.empty())
				{
					pair<int, int> top = order_idx_pq.top();
					idx_order.push_back(top.second);

					order_idx_pq.pop();
				}

			}

			// null comes before other idx
			int tt = 0;
			for (tt = 0; tt < null_order.size(); tt++)
			{
				fin_order.push_back(null_order[tt]);
			}
			tt = 0;
			for (tt = 0; tt < idx_order.size(); tt++)
			{
				fin_order.push_back(idx_order[tt]);
			}

		}
		else 
		{
			if (col_type == T_CHAR || col_type == T_VARCHAR)
			{
				priority_queue< pair<string, int> > order_idx_pq;

				// iterate entire col and push the val and idx
				int c_itr = 0;
				vector<void *> o_col = full_table_data[o_col_idx];

				for (c_itr = 0; c_itr < o_col.size(); c_itr++)
				{
					char *str_size_char = (char*)o_col[c_itr];
					int str_size = (int)*str_size_char;

					if (str_size == 0)
					{
						null_order.push_back(c_itr);
						continue;
					}
					else
					{
						char *field_start_char = ((char *)o_col[c_itr]) + 1;
						char field_val[str_size];

						memset(&field_val, '\0', str_size);
						memcpy(&field_val, field_start_char, str_size);
						// field_val[str_size] = '\0';

						string in_str = string(field_val);

						order_idx_pq.push(make_pair(in_str, c_itr));
					}

				}

				// pop all the order_pq values and push into idx order
				while (!order_idx_pq.empty())
				{
					pair<string, int> top = order_idx_pq.top();
					idx_order.push_back(top.second);

					order_idx_pq.pop();
				}

			}
			else
			{
				priority_queue< pair<int, int> > order_idx_pq;

				// iterate entire col and push the val and idx
				int c_itr = 0;
				vector<void *> o_col = full_table_data[o_col_idx];

				for (c_itr = 0; c_itr < o_col.size(); c_itr++)
				{
					char *int_field_size_char = (char *)o_col[c_itr];
					int int_field_size = (int)*int_field_size_char;

					if (int_field_size == 0)
					{
						null_order.push_back(c_itr);
						continue;
					}
					else
					{
						int *field_val = (int *)((char*)o_col[c_itr] + 1);
						int in_field = *field_val;
						order_idx_pq.push(make_pair(in_field, c_itr));
					}

				}

				// pop all the order_pq values and push into idx order
				while (!order_idx_pq.empty())
				{
					pair<int, int> top = order_idx_pq.top();
					idx_order.push_back(top.second);

					order_idx_pq.pop();
				}

			}

			// null comes after other idx
			int tt = 0;
			for (tt = 0; tt < idx_order.size(); tt++)
			{
				fin_order.push_back(idx_order[tt]);
			}
			tt = 0;
			for (tt = 0; tt < null_order.size(); tt++)
			{
				fin_order.push_back(null_order[tt]);
			}

		}
		
	}
	else {
		// regular ordering
		int tt = 0;
		for (tt = 0; tt < col_height; tt++)
		{
			fin_order.push_back(tt);
		}
	}

	// int tt = 0;
	// printf("fin_order:");
	// for (tt = 0; tt < fin_order.size(); tt++)
	// {
	// 	printf("%d ,", fin_order[tt]);
	// }
	// printf("\n");

	i = 0;
	int k = 0;

	vector<bool> display_flag;


	if (wh_col_list.size() != 0) {
		for (k = 0; k < col_height; k++)
		{
			bool final_val = false;
			vector<bool> res_list;
			for (i = 0; i < full_table_data.size(); i++)
			{
				int col_idx = i;
				int col_type = col_arr[col_idx].col_type;
				// vector<bool> res_list;
				// iterate through all the wh col list until you hit this column
				int ii = 0;
				for (ii = 0; ii < wh_col_list.size(); ii++)
				{
					
					if (col_idx == wh_col_list[ii])
					{
						char *const_val = wh_const_list[ii];
						char *rel_sym = wh_sym_list[ii];

						int rel_flag = -1;
						bool this_res;

						if (strcmp(rel_sym, "<") == 0)
						{
							rel_flag = 0;
						}
						else if (strcmp(rel_sym, ">") == 0) 
						{
							rel_flag = 1;
						}
						else if (strcmp(rel_sym, "=") == 0)
						{
							rel_flag = 2;
						}
						else if (strcmp(rel_sym, "is") == 0)
						{
							rel_flag = 3;
						}
						else if (strcmp(rel_sym, "isnot") == 0)
						{
							rel_flag = 4;
						}

						vector<void *> cur_col_data = full_table_data[i];
						if (col_type == T_CHAR || col_type == T_VARCHAR) {
							char *str_size_char = (char*)cur_col_data[k];
							int str_size = (int)*str_size_char;

							if (str_size == 0)
							{
								if (rel_flag == 3)
								{
									res_list.push_back(true);
								}
								else if (rel_flag == 4)
								{
									res_list.push_back(false);
								}
								else {
									res_list.push_back(false);
								}
								continue;
							}

							char *field_start_char = ((char *)cur_col_data[k]) + 1;
							char field_val[str_size+1];

							memset(&field_val, '\0', str_size+1);
							memcpy(&field_val, field_start_char, str_size);
							field_val[str_size] = '\0';

							if (rel_flag == 2)
							{
								if (strcmp(field_val, const_val) == 0)
								{
									res_list.push_back(true);
								}
								else
								{
									res_list.push_back(false);
								}
							}
							else if (rel_flag == 0) {
								if (strcmp(field_val, const_val) < 0)
								{
									res_list.push_back(true);
								}
								else
								{
									res_list.push_back(false);
								}
							}
							else if (rel_flag == 1) {
								if (strcmp(field_val, const_val) > 0)
								{
									res_list.push_back(true);
								}
								else
								{
									res_list.push_back(false);
								}

							}
							else if (rel_flag == 3)
							{
								res_list.push_back(false);
							}
							else if (rel_flag == 4)
							{
								res_list.push_back(true);
							}

						}
						else {
							
							char *int_field_size_char = (char *)cur_col_data[k];
							int int_field_size = (int)*int_field_size_char;

							if (int_field_size == 0)
							{
								if (rel_flag == 3)
								{
									res_list.push_back(true);
								}
								else if (rel_flag == 4)
								{
									res_list.push_back(false);
								}
								else {
									res_list.push_back(false);
								}

								continue;
							}

							int *field_val = (int *)((char*)cur_col_data[k] + 1);
							int int_const_val = atoi(const_val);
							// printf("%d ", int_const_val);

							if (rel_flag == 0)
							{
								if (*field_val < int_const_val)
								{
									res_list.push_back(true);
								}
								else {
									res_list.push_back(false);
								}

							}
							else if (rel_flag == 1)
							{
								if (*field_val > int_const_val)
								{
									res_list.push_back(true);
								}
								else {
									res_list.push_back(false);
								}

							}
							else if (rel_flag == 2)
							{
								if (*field_val == int_const_val)
								{
									res_list.push_back(true);
								}
								else {
									res_list.push_back(false);
								}

							}
							else if (rel_flag == 3)
							{
								res_list.push_back(false);
							}
							else if (rel_flag == 4)
							{
								res_list.push_back(true);
							}

						}

					}
				}

			}

			if (res_list.size() == 1)
			{
				display_flag.push_back(res_list[0]);
				continue;
			}

			int kk = 0;
			bool fin_start = res_list[0];
			for (kk = 1; kk < res_list.size(); kk++)
			{
				int cur_cond = wh_cond_list[kk-1];

				if (cur_cond == K_AND)
				{
					fin_start = fin_start && res_list[kk];
				} else {
					fin_start = fin_start || res_list[kk];
				}
			}
			final_val = fin_start;
			display_flag.push_back(final_val);
		}

	}

	i = 0;
	j = 0;
	k = 0;

	int table_width = 0;
	int aggre_table_width = 0;
	// print all column headers
	printf("\n");
	if (func_code == -1)
	{
		table_width = 0;
		for (i = 0; i < col_idx_list.size(); i++)
		{
			int col_idx = col_idx_list[i];
			char *col_name = col_arr[col_idx].col_name;

			int f_col_width = col_width_arr[col_idx];
			table_width = table_width + f_col_width + 3;

		}

		printf(" +");
		printf("%s", string(table_width-1, '-').c_str());
		printf("%s\n", string(1, '+').c_str());

		for (i = 0; i < col_idx_list.size(); i++)
		{
			int col_idx = col_idx_list[i];
			char *col_name = col_arr[col_idx].col_name;

			int f_col_width = col_width_arr[col_idx];

			string s1 = "%-";
			string s2 = to_string(f_col_width);
			string s3 = "s";
			string format_string = s1 + s2 + s3;

			int f_len = format_string.length();
			char f_str[f_len+1];

			int s_itr = 0;
			for (s_itr = 0; s_itr < f_len; s_itr++)
			{
				f_str[s_itr] = format_string[s_itr];
			}

			f_str[f_len] = '\0';


			if (i == 0)
			{
				printf(" | ");
			}

			printf(f_str,col_name);
			printf(" | ");
		}
		printf("\n");

		printf(" +");
		printf("%s", string(table_width-1, '-').c_str());
		printf("%s", string(1, '+').c_str());
	}
	else {

		// for (i = 0; i < col_idx_list.size(); i++)
		// {
		// 	int col_idx = col_idx_list[i];
		// 	char *col_name = col_arr[col_idx].col_name;

		// 	if (func_code == F_COUNT)
		// 	{
		// 		printf("COUNT(%s) ",col_name);
		// 	}
		// 	else if (func_code == F_SUM)
		// 	{
		// 		printf("SUM(%s) ",col_name);
		// 	}
		// 	else if (func_code == F_AVG)
		// 	{
		// 		printf("AVG(%s) ",col_name);
		// 	}
		// }
	}

	printf("\n");

	k = 0;
	i = 0;
	j = 0;

	if (func_code == -1) {
		for (k = 0; k < col_height; k++)
		{
			int o_itr = fin_order[k];
			if (wh_col_list.size() != 0)
			{
				if (!display_flag[o_itr])
					continue;
			}

			for (i = 0; i < table_data.size(); i++)
			{
				if (i == 0)
				{
					printf(" | ");
				}

				int col_idx = col_idx_list[i];
				int col_type = col_arr[col_idx].col_type;

				// printf("%s:\n", col_arr[col_idx].col_name);

				vector<void *> cur_col_data = table_data[i];

				//formatting output
				int f_col_width = col_width_arr[col_idx];
				string s1 = "%-";
				string s11 = "%";
				string s2 = to_string(f_col_width);
				string s3 = "s";
				string s4 = "d";
				string format_string_str = s1 + s2 + s3;
				string format_string_int = s11 + s2 + s4;

				int f_len_str = format_string_str.length();
				int f_len_int = format_string_int.length();

				char f_str[f_len_str+1];
				char f_int[f_len_int+1];

				int s_itr = 0;
				for (s_itr = 0; s_itr < f_len_str; s_itr++)
				{
					f_str[s_itr] = format_string_str[s_itr];
				}
				for (s_itr = 0; s_itr < f_len_int; s_itr++)
				{
					f_int[s_itr] = format_string_int[s_itr];
				}

				f_str[f_len_str] = '\0';
				f_int[f_len_int] = '\0';

				if (col_type == T_CHAR || col_type == T_VARCHAR) {
					char *str_size_char = (char*)cur_col_data[o_itr];
					int str_size = (int)*str_size_char;
					char *field_start_char = ((char *)cur_col_data[o_itr]) + 1;
					char field_val[str_size+1];

					// printf("str_size: %d\n", str_size);
					memset(&field_val, '\0', str_size+1);
					if (str_size != 0)
						memcpy(&field_val, field_start_char, str_size);
					field_val[str_size] = '\0';

					if (str_size == 0)
					{
						// printf("Null ");
						char *null_str = "Null";
						printf(f_str, null_str);

					}
					else {
						// printf("%s ", field_val);
						printf(f_str, field_val);
					}
				}
				else {
					char *int_field_size_char = (char *)cur_col_data[o_itr];
					int int_field_size = (int)*int_field_size_char;

					if (int_field_size != 0)
					{
						int *field_val = (int *)((char*)cur_col_data[o_itr] + 1);
						// printf("%d ", *field_val);
						printf(f_int, *field_val);

					}
					else
					{
						char *null_str = "Null";
						printf(f_str, null_str);
					}
				}
				printf(" | ");
			}
			printf("\n");
		}
		printf(" +");
		printf("%s", string(table_width-1, '-').c_str());
		printf("%s", string(1, '+').c_str());
		printf("\n");
		printf("\n");
	}
	else {
		// setup aggregator results
		int i_idx = 0;
		vector<int> sum_res;
		vector<int> count_res;
		vector<int> avg_res;
		vector<int> full_count_res;

		// format output
		vector<int> agre_col_width_arr;
		aggre_table_width = 0;
		int c_star_tab_width = 0;
		for (i = 0; i < table_data.size(); i++)
		{
			int col_idx = col_idx_list[i];
			char* agre_col_name = col_arr[col_idx].col_name;
			int fin_width = 0;

			if (func_code == F_COUNT)
			{
				fin_width = 7 + strlen(agre_col_name);
				if (fin_width < 10)
				{
					fin_width = 10;
				}
			}
			else if (func_code == F_SUM)
			{
				fin_width = 5 + strlen(agre_col_name);
				if (fin_width < 10)
				{
					fin_width = 10;
				}
			}
			else if (func_code == F_AVG)
			{
				fin_width = 5 + strlen(agre_col_name);

				if (fin_width < 10)
				{
					fin_width = 10;
				}
			}
			agre_col_width_arr.push_back(fin_width);

			aggre_table_width = aggre_table_width + fin_width + 3;
		}

		// count(*) --> only case where more than 1 col considered
		if (col_idx_list.size() > 1)
		{
			string count_star_str = "COUNT(*)";
			c_star_tab_width = count_star_str.length() + 3;

			printf(" +");
			printf("%s", string(c_star_tab_width-1, '-').c_str());
			printf("%s", string(1, '+').c_str());
			printf("\n");

			printf(" | ");
			printf("%-8s", count_star_str.c_str());
			printf(" | ");

			printf("\n");
			printf(" +");
			printf("%s", string(c_star_tab_width-1, '-').c_str());
			printf("%s", string(1, '+').c_str());
			printf("\n");
		}
		else 
		{
			printf(" +");
			printf("%s", string(aggre_table_width-1, '-').c_str());
			printf("%s", string(1, '+').c_str());
			printf("\n");
			
			for (i = 0; i < col_idx_list.size(); i++)
			{
				if (i == 0)
				{
					printf(" | ");
				}
				int col_idx = col_idx_list[i];
				char *col_name = col_arr[col_idx].col_name;

				string raw_col_name = string(col_name);
				string aggre_col_name = "";

				if (func_code == F_COUNT)
				{
					aggre_col_name = "COUNT(" + raw_col_name + ")";
				}
				else if (func_code == F_SUM)
				{
					aggre_col_name = "SUM(" + raw_col_name + ")";
				}
				else if (func_code == F_AVG)
				{
					aggre_col_name = "AVG(" + raw_col_name + ")";
				}

				int a_col_width = agre_col_width_arr[i];

				string s1 = "%-";
				string s11 = "%";
				string s2 = to_string(a_col_width);
				string s3 = "s";
				string s4 = "d";
				string format_string_str = s1 + s2 + s3;
				string format_string_int = s11 + s2 + s4;

				int f_len_str = format_string_str.length();
				int f_len_int = format_string_int.length();

				char f_str[f_len_str+1];
				char f_int[f_len_int+1];

				int s_itr = 0;
				for (s_itr = 0; s_itr < f_len_str; s_itr++)
				{
					f_str[s_itr] = format_string_str[s_itr];
				}
				for (s_itr = 0; s_itr < f_len_int; s_itr++)
				{
					f_int[s_itr] = format_string_int[s_itr];
				}

				f_str[f_len_str] = '\0';
				f_int[f_len_int] = '\0';

				printf(f_str, aggre_col_name.c_str());
				printf(" | ");

			}
			
			printf("\n");
			printf(" +");
			printf("%s", string(aggre_table_width-1, '-').c_str());
			printf("%s", string(1, '+').c_str());
			printf("\n");

		}
		for (i_idx = 0; i_idx < table_data.size(); i_idx++)
		{
			sum_res.push_back(0);
			count_res.push_back(0);
			full_count_res.push_back(0);
		}

		for (k = 0; k < col_height; k++)
		{

			if (wh_col_list.size() != 0)
			{
				if (!display_flag[k])
					continue;
			}

			for (i = 0; i < table_data.size(); i++)
			{
				int col_idx = col_idx_list[i];
				int col_type = col_arr[col_idx].col_type;

				// printf("%s:\n", col_arr[col_idx].col_name);

				vector<void *> cur_col_data = table_data[i];

				if (col_type == T_CHAR || col_type == T_VARCHAR) {
					char *str_size_char = (char*)cur_col_data[k];
					int str_size = (int)*str_size_char;
					char *field_start_char = ((char *)cur_col_data[k]) + 1;
					char field_val[str_size+1];

					// don't consider null values
					if (str_size == 0)
					{
						full_count_res[i] = full_count_res[i] + 1;
						continue;
					}

					// printf("str_size: %d\n", str_size);
					memset(&field_val, '\0', str_size+1);
					if (str_size != 0)
						memcpy(&field_val, field_start_char, str_size);
					field_val[str_size] = '\0';

					if (func_code == F_COUNT)
					{
						count_res[i] = count_res[i] + 1;
						full_count_res[i] = full_count_res[i] + 1;
					}
				}
				else {
					char *int_field_size_char = (char *)cur_col_data[k];
					int int_field_size = (int)*int_field_size_char;

					// don't consider null values
					if (int_field_size == 0)
					{
						full_count_res[i] = full_count_res[i] + 1;
						continue;
					}

					if (func_code == F_COUNT)
					{
						count_res[i] = count_res[i] + 1;
						full_count_res[i] = full_count_res[i] + 1;	
					}
					else if (func_code == F_SUM || func_code == F_AVG )
					{
						if (int_field_size != 0)
						{
							int *field_val = (int *)((char*)cur_col_data[k] + 1);
							sum_res[i] = sum_res[i] + *field_val;
							count_res[i] = count_res[i] + 1;
						}						
					}
				}

			}

		}

		// as of now only count(*) can get more than 1 column
		if (col_idx_list.size() > 1)
		{
			int max_rows = 0;
			for (int a_itr = 0; a_itr < full_count_res.size(); a_itr++)
			{
				int cur_r_cnt = full_count_res[a_itr];
				if (max_rows < cur_r_cnt)
				{
					max_rows = cur_r_cnt;
				}
			}

			printf(" | ");
			printf("%8d", max_rows);
			printf(" | ");

			printf("\n");
			printf(" +");
			printf("%s", string(c_star_tab_width-1, '-').c_str());
			printf("%s", string(1, '+').c_str());
			printf("\n");
			printf("\n");
			return 0;

		}

		// count
		if (func_code == F_COUNT)
		{
			int a_itr = 0;
			
			for (a_itr = 0; a_itr < count_res.size(); a_itr++)
			{
				if (a_itr == 0)
				{
					printf(" | ");
				}
				int a_col_width = agre_col_width_arr[a_itr];

				string s11 = "%";
				string s2 = to_string(a_col_width);
				string s4 = "d";
				string format_string_int = s11 + s2 + s4;

				int f_len_int = format_string_int.length();
				char f_int[f_len_int+1];

				int s_itr = 0;
				for (s_itr = 0; s_itr < f_len_int; s_itr++)
				{
					f_int[s_itr] = format_string_int[s_itr];
				}

				f_int[f_len_int] = '\0';

				printf(f_int, count_res[a_itr]);
				printf(" | ");
			}

			printf("\n");
			printf(" +");
			printf("%s", string(aggre_table_width-1, '-').c_str());
			printf("%s", string(1, '+').c_str());
			printf("\n");
			printf("\n");
			return 0;
		}

		// avg
		if (func_code == F_SUM)
		{
			int a_itr = 0;
			for (a_itr = 0; a_itr < sum_res.size(); a_itr++)
			{
				int a_col_width = agre_col_width_arr[a_itr];

				if (a_itr == 0)
				{
					printf(" | ");
				}

				string s11 = "%";
				string s2 = to_string(a_col_width);
				string s4 = "d";
				string format_string_int = s11 + s2 + s4;

				int f_len_int = format_string_int.length();
				char f_int[f_len_int+1];

				int s_itr = 0;
				for (s_itr = 0; s_itr < f_len_int; s_itr++)
				{
					f_int[s_itr] = format_string_int[s_itr];
				}

				f_int[f_len_int] = '\0';

				printf(f_int, sum_res[a_itr]);
				printf(" | ");
			}
			
			printf("\n");
			printf(" +");
			printf("%s", string(aggre_table_width-1, '-').c_str());
			printf("%s", string(1, '+').c_str());
			printf("\n");
			printf("\n");
			return 0;
		}

		// avg
		if (func_code == F_AVG)
		{
			int a_itr = 0;
			for (a_itr = 0; a_itr < sum_res.size(); a_itr++)
			{
				int a_col_width = agre_col_width_arr[a_itr];

				if (a_itr == 0)
				{
					printf(" | ");
				}

				string s11 = "%";
				string s2 = to_string(a_col_width);
				string s4 = "f";
				string format_string_int = s11 + s2 + s4;

				int f_len_int = format_string_int.length();
				char f_int[f_len_int+1];

				int s_itr = 0;
				for (s_itr = 0; s_itr < f_len_int; s_itr++)
				{
					f_int[s_itr] = format_string_int[s_itr];
				}

				f_int[f_len_int] = '\0';
				printf(f_int, (float)sum_res[a_itr]/(float)count_res[a_itr]);
				printf(" | ");
			}

			printf("\n");
			printf(" +");
			printf("%s", string(aggre_table_width-1, '-').c_str());
			printf("%s", string(1, '+').c_str());
			printf("\n");
			printf("\n");
			return 0;
		}

	}

free_full_table_data:
	for (int c_itr = 0; c_itr < full_table_data.size(); c_itr++)
	{
		int t_itr = 0;
		vector<void *> col_data = full_table_data[c_itr];
		for (t_itr = 0; t_itr < col_data.size();t_itr++)
		{
			if (col_data[t_itr] != NULL)
			{
				free(col_data[t_itr]);
				col_data[t_itr] = NULL;
			}
		}
	}
free_table_data:
	for (int c_itr = 0; c_itr < table_data.size(); c_itr++)
	{
		int t_itr = 0;
		vector<void *> col_data = table_data[c_itr];
		for (t_itr = 0; t_itr < col_data.size();t_itr++)
		{
			if (col_data[t_itr] != NULL)
			{
				free(col_data[t_itr]);
				col_data[t_itr] = NULL;
			}
		}
	}
free_table_file_data:
	free(table_file_data);
	return 0;
}

int sem_delete(token_list *t_list)
{
	int rc = 0;
	token_list *cur;
	tpd_entry *new_entry = NULL;
	cur = t_list;

	if (cur->tok_value != K_FROM)
	{
		rc = INVALID_STATEMENT;
		cur->tok_value = INVALID;
		return rc;
	}

	cur  = cur->next;

	if (cur->tok_class != identifier)
	{
		rc = INVALID_STATEMENT;
		cur->tok_value = INVALID;
		return rc;
	}

	// parse table name
	char *table_name = cur->tok_string;

	// check if table name exists
	if ((new_entry = get_tpd_from_list(table_name)) == NULL)
    {
		rc = TABLE_NOT_EXIST;
		cur->tok_value = INVALID;
		return rc;
    }
	else {

		cur = cur->next;
		if (cur->tok_value != K_WHERE)
		{
			rc = INVALID_STATEMENT;
			cur->tok_value = INVALID;
			return rc;
		}

		cur = cur->next;
		if (cur->tok_class != identifier)
		{
			rc = INVALID_STATEMENT;
			cur->tok_value = INVALID;
			return rc;
		}

		// get column name
		char *col_name = cur->tok_string;
		cd_entry *col_arr = (cd_entry*)(((char *)new_entry) + new_entry->cd_offset);

		// check if col exists in the table
		int idx = 0;
		int f_col_idx = -1;
		for (idx = 0; idx < new_entry->num_columns; idx++)
		{
			char *i_col_name = col_arr[idx].col_name;

			if (strcmp(col_name, i_col_name) == 0)
			{
				f_col_idx = idx;
			}	

		}

		if (f_col_idx == -1)
		{
			rc = COLUMN_NOT_EXIST;
			cur->tok_value = INVALID;
			return rc;
		}

		// get symbols < > = or keywords IS NOT NULL, IS NULL
		cur = cur->next;
		if (cur->tok_class != symbol && cur->tok_class != keyword)
		{
			rc = INVALID_STATEMENT;
			cur->tok_value = INVALID;
			return rc;
		}

		if (cur->tok_class == symbol)
		{
			// get symbol
			if (cur->tok_value != S_GREATER && cur->tok_value != S_LESS &&
				cur->tok_value != S_EQUAL)
			{
				rc = INVALID_RELATIONAL_OPERATOR;
				cur->tok_value = INVALID;
				return rc;
			}

			char cur_sym[strlen(cur->tok_string) + 1];
			strcpy(cur_sym, cur->tok_string);

			cur->tok_string;
			// printf("cur_sym %s\n", cur_sym);

			cur = cur->next;
			if (cur->tok_class != constant)
			{
				rc = INVALID_STATEMENT;
				cur->tok_value = INVALID;
				return rc;
			}

			if (cur->tok_value != STRING_LITERAL && cur->tok_value != INT_LITERAL)
			{
				rc = INVALID_STATEMENT;
				cur->tok_value = INVALID;
				return rc;
			}

			int dt_col_type = col_arr[f_col_idx].col_type;
			if (cur->tok_value == STRING_LITERAL)
			{
				if (dt_col_type != T_CHAR && dt_col_type != T_VARCHAR)
				{
					rc = DATATYPE_MISMATCH_ERROR;
					cur->tok_value = INVALID;
					return rc;
				}
				
			}
			else {
				if (dt_col_type != T_INT)
				{
					rc = DATATYPE_MISMATCH_ERROR;
					cur->tok_value = INVALID;
					return rc;
				}

			}
			// printf("cur_sym %s\n", cur_sym);
			// call delete here
			rc = delete_record_from_table(new_entry, f_col_idx, cur_sym, cur->tok_string);
			return rc;
		}
		else 
		{
			// key words
			if (cur->tok_value != K_IS)
			{
				rc = INVALID_STATEMENT;
				cur->tok_value = INVALID;
				return rc;
			}

			cur = cur->next;

			if (cur->tok_value != K_NOT && cur->tok_value != K_NULL)
			{
				rc = INVALID_STATEMENT;
				cur->tok_value = INVALID;
				return rc;
			}

			char *cur_is_sym = "is";
			char *cur_is_not_sym = "isnot";
			
			int col_idx = f_col_idx;

			if (cur->tok_value == K_NOT)
			{
				cur = cur->next;
				if (cur->tok_value != K_NULL)
				{
					rc = INVALID_STATEMENT;
					cur->tok_value = INVALID;
					return rc;
				}

				// is not null
				rc = delete_record_from_table(new_entry, f_col_idx, cur_is_not_sym,
				 cur->tok_string);
				return rc;

			}
			else {
				// is null
				rc = delete_record_from_table(new_entry, f_col_idx, cur_is_sym,
				 cur->tok_string);
				return rc;	
			}

		}

		// get end of char
		cur = cur->next;
		if (cur->tok_value != EOC)
		{
			rc = INVALID_STATEMENT;
			cur->tok_value = INVALID;
			return rc;
		}

	}


	return rc;
}

int delete_record_from_table(tpd_entry *new_entry, int col_idx, char *rel_sym, char *const_val)
{
	int rc = 0;

	// printf("col_idx %d\n", col_idx);
	// printf("rel_sym %s\n", rel_sym);
	// printf("const_val %s\n", const_val);


	cd_entry *col_arr = (cd_entry*)(((char *)new_entry) + new_entry->cd_offset);
	void *table_file_data = get_table_file_data(new_entry->table_name);

	table_file_header* tf_hdr = (table_file_header*)table_file_data;
	int rec_offset = tf_hdr->record_offset;
	int rec_size = tf_hdr->record_size;
	int num_cols = new_entry->num_columns;
	char *rec_mem = ((char *)table_file_data + rec_offset);
	
	char *cur_ptr = rec_mem;
	char *end_ptr = ((char *)table_file_data + tf_hdr->file_size);

	vector<void *> updated_rec_list;
	int del_count = 0;

	while (cur_ptr != end_ptr)
	{
		void *cur_rec_mem = calloc(1, rec_size);
		char *temp_ptr = cur_ptr;
		int idx = 0;
		bool del_flag = false;
		for (idx = 0; idx <= col_idx; idx++)
		{
			// get size
			int col_type = col_arr[idx].col_type;
			

			int field_size = (int)*(char *)temp_ptr;

			if (idx < col_idx)
			{
				temp_ptr = temp_ptr + field_size + 1;
				continue;
			}

			int rel_flag = -1;

			if (strcmp(rel_sym, "<") == 0)
			{
				rel_flag = 0;
			}
			else if (strcmp(rel_sym, ">") == 0) 
			{
				rel_flag = 1;
			}
			else if (strcmp(rel_sym, "=") == 0)
			{
				rel_flag = 2;
			}
			else if (strcmp(rel_sym, "is") == 0)
			{
				rel_flag = 3;
			}
			else if (strcmp(rel_sym, "isnot") == 0)
			{
				rel_flag = 4;
			}

			if (col_type == T_CHAR || col_type == T_VARCHAR)
			{
				char *str_size_char = (char*) temp_ptr;
				int str_size = (int)*temp_ptr;

				if (str_size == 0)
				{
					if (rel_flag == 3)
					{
						del_flag = true;
					}
					else if (rel_flag == 4)
					{
						del_flag = false;
					}
					else {
						del_flag = false;
					}

					temp_ptr = temp_ptr + field_size + 1;
					continue;
				}

				char *field_start_char = temp_ptr + 1;
				char field_val[str_size+1];

				memset(&field_val, '\0', str_size+1);
				memcpy(&field_val, field_start_char, str_size);
				field_val[str_size] = '\0';

				if (rel_flag == 2)
				{
					if (strcmp(field_val, const_val) == 0)
					{
						del_flag = true;
					}
					else
					{
						del_flag = false;
					}
				}
				else if (rel_flag == 0) {
					if (strcmp(field_val, const_val) < 0)
					{
						del_flag = true;
					}
					else
					{
						del_flag = false;
					}
				}
				else if (rel_flag == 1) {

					if (strcmp(field_val, const_val) > 0)
					{
						del_flag = true;
					}
					else
					{
						del_flag = false;
					}

				}
				else if (rel_flag == 3)
				{
					del_flag = false;
				}
				else if (rel_flag == 4)
				{
					del_flag = true;
				}

				temp_ptr = temp_ptr + field_size + 1;

			}
			else {

				char *int_field_size_char = (char *)temp_ptr;
				int int_field_size = (int)*int_field_size_char;

				if (int_field_size == 0)
				{
					if (rel_flag == 3)
					{
						del_flag = true;
					}
					else if (rel_flag == 4)
					{
						del_flag = false;
					}
					else {

						del_flag = false;
					}

					temp_ptr = temp_ptr + field_size + 1;
					continue;
				}

				int *field_val = (int *)(temp_ptr + 1);
				int int_const_val = atoi(const_val);

				// temp_ptr = temp_ptr + 1 + int_field_size;

				if (rel_flag == 0)
				{
					if (*field_val < int_const_val)
					{
						del_flag = true;
					}
					else {
						del_flag = false;
					}

				}
				else if (rel_flag == 1)
				{
					if (*field_val > int_const_val)
					{
						del_flag = true;
					}
					else {
						del_flag = false;
					}

				}
				else if (rel_flag == 2)
				{
					if (*field_val == int_const_val)
					{
						del_flag = true;
					}
					else {
						del_flag = false;
					}
				}
				else if (rel_flag == 3)
				{
					del_flag = false;
				}
				else if (rel_flag == 4)
				{
					del_flag = true;
				}

				temp_ptr = temp_ptr + field_size + 1;
			}
			
			
		}

		if (!del_flag)
		{
			memcpy(cur_rec_mem, cur_ptr, rec_size);
			updated_rec_list.push_back(cur_rec_mem);
		} else {
			del_count++;
		}
		cur_ptr = cur_ptr + rec_size;
	}

	// printf("del_count %d\n", del_count);
	// update header file fields

	int new_num_records = tf_hdr->num_records - del_count;
	int new_file_size = tf_hdr->file_size - (del_count * rec_size);
	tf_hdr->num_records = new_num_records;
	tf_hdr->file_size = new_file_size;
	
	char file_table_name[MAX_IDENT_LEN+4];
	void *mem_ptr = NULL;
	FILE *fhandle = NULL;

	memset(file_table_name, 0, MAX_IDENT_LEN+4);
	strcpy(file_table_name, new_entry->table_name);
	strcat(file_table_name, ".tab");

	// write header followed by all the void * mem pointers one by one into file
	if((fhandle = fopen(file_table_name, "wbc")) == NULL) {
		rc = FILE_OPEN_ERROR;
		return rc;
	}
	else {
		fwrite((void *)tf_hdr, rec_offset, 1, fhandle);

		int ii = 0;

		for (ii = 0; ii < updated_rec_list.size(); ii++)
		{
			fwrite((void *)updated_rec_list[ii], rec_size, 1, fhandle);
		}

		fflush(fhandle);
		fclose(fhandle);
	}

	if (del_count == 0)
	{
		printf("Warning: 0 rows deleted.\n\n");
	}
	else
	{
		printf("Deleted %d rows successfully.\n\n", del_count);
	}

	printf("Table File Header (Latest):\n");
	printf(" file_size: %d\n", tf_hdr->file_size);
	printf(" record_offset: %d\n", tf_hdr->record_offset);
	printf(" num_records: %d\n", tf_hdr->num_records);
	printf(" record_size: %d\n\n", tf_hdr->record_size);

free_updated_rec_list_mem:
	for (int c_itr = 0; c_itr < updated_rec_list.size(); c_itr++)
	{
		if (updated_rec_list[c_itr] != NULL)
		{
			free(updated_rec_list[c_itr]);
		}
	}
func_ret:
	return rc;
}

int sem_update(token_list *t_list)
{
	int rc = 0;
	token_list *cur;
	tpd_entry *new_entry = NULL;
	cur = t_list;
	int s_val_null_flag = 0;

	if (cur->tok_class != identifier)
	{
		rc = INVALID_STATEMENT;
		cur->tok_value = INVALID;
		return rc;
	}

	// parse table name
	char *table_name = cur->tok_string;

	// check if table name exists
	if ((new_entry = get_tpd_from_list(table_name)) == NULL)
    {
		rc = TABLE_NOT_EXIST;
		cur->tok_value = INVALID;
		return rc;
    }
	else {
		cd_entry *col_arr = (cd_entry*)(((char *)new_entry) + new_entry->cd_offset);
		int idx = 0;
		int f_col_idx = -1;
		int s_col_idx = -1;

		cur = cur->next;
		if (cur->tok_value != K_SET)
		{
			rc = INVALID_STATEMENT;
			cur->tok_value = INVALID;
			return rc;
		}

		cur = cur->next;

		// get col name 
		if (cur->tok_class != identifier)
		{
			rc = INVALID_STATEMENT;
			cur->tok_value = INVALID;
			return rc;
		}

		char *s_col_name = cur->tok_string;
		for (idx = 0; idx < new_entry->num_columns; idx++)
		{
			char *i_col_name = col_arr[idx].col_name;

			if (strcmp(s_col_name, i_col_name) == 0)
			{
				s_col_idx = idx;
			}	

		}

		if (s_col_idx == -1)
		{
			rc = COLUMN_NOT_EXIST;
			cur->tok_value = INVALID;
			return rc;
		}

		// parse symbol
		cur = cur->next;
		if (cur->tok_value != S_EQUAL)
		{
			rc = INVALID_STATEMENT;
			cur->tok_value = INVALID;
			return rc;
		}

		cur = cur->next;
		// get value
		if (cur->tok_class != constant && cur->tok_value != K_NULL)
		{
			rc = INVALID_STATEMENT;
			cur->tok_value = INVALID;
			return rc;
		}

		if (cur->tok_value != STRING_LITERAL && cur->tok_value != INT_LITERAL && 
			cur->tok_value != K_NULL)
		{
			rc = INVALID_STATEMENT;
			cur->tok_value = INVALID;
			return rc;
		}

		int dt_col_type = col_arr[s_col_idx].col_type;
		int dt_not_null_flag = col_arr[s_col_idx].not_null;
		if (cur->tok_value == STRING_LITERAL)
		{
			if (dt_col_type != T_CHAR && dt_col_type != T_VARCHAR)
			{
				rc = DATATYPE_MISMATCH_ERROR;
				cur->tok_value = INVALID;
				return rc;
			}
			
		}
		else if (cur->tok_value == INT_LITERAL) 
		{
			if (dt_col_type != T_INT)
			{
				rc = DATATYPE_MISMATCH_ERROR;
				cur->tok_value = INVALID;
				return rc;
			}

		}
		else {

			// check if col has not null flag set
			if (dt_not_null_flag)
			{
				rc = INPUT_DATA_TYPE_NOT_NULL_ERROR;
				cur->tok_value = INVALID;
				return rc;
			}

			s_val_null_flag = 1;
		}

		char *s_val = cur->tok_string;

		cur = cur->next;
		if (cur->tok_value != K_WHERE)
		{
			rc = INVALID_STATEMENT;
			cur->tok_value = INVALID;
			return rc;
		}

		cur = cur->next;
		if (cur->tok_class != identifier)
		{
			rc = INVALID_STATEMENT;
			cur->tok_value = INVALID;
			return rc;
		}

		// get column name
		char *col_name = cur->tok_string;
		

		// check if col exists in the table

		for (idx = 0; idx < new_entry->num_columns; idx++)
		{
			char *i_col_name = col_arr[idx].col_name;

			if (strcmp(col_name, i_col_name) == 0)
			{
				f_col_idx = idx;
			}	

		}

		if (f_col_idx == -1)
		{
			rc = COLUMN_NOT_EXIST;
			cur->tok_value = INVALID;
			return rc;
		}

		// get symbols < > = or keywords IS NOT NULL, IS NULL
		cur = cur->next;
		if (cur->tok_class != symbol && cur->tok_class != keyword)
		{
			rc = INVALID_STATEMENT;
			cur->tok_value = INVALID;
			return rc;
		}

		if (cur->tok_class == symbol)
		{
			// get symbol
			if (cur->tok_value != S_GREATER && cur->tok_value != S_LESS &&
				cur->tok_value != S_EQUAL)
			{
				rc = INVALID_RELATIONAL_OPERATOR;
				cur->tok_value = INVALID;
				return rc;
			}

			char cur_sym[strlen(cur->tok_string) + 1];
			strcpy(cur_sym, cur->tok_string);

			cur->tok_string;
			// printf("cur_sym %s\n", cur_sym);

			cur = cur->next;
			if (cur->tok_class != constant)
			{
				rc = INVALID_STATEMENT;
				cur->tok_value = INVALID;
				return rc;
			}

			if (cur->tok_value != STRING_LITERAL && cur->tok_value != INT_LITERAL)
			{
				rc = INVALID_STATEMENT;
				cur->tok_value = INVALID;
				return rc;
			}

			int dt_col_type = col_arr[f_col_idx].col_type;
			if (cur->tok_value == STRING_LITERAL)
			{
				if (dt_col_type != T_CHAR && dt_col_type != T_VARCHAR)
				{
					rc = DATATYPE_MISMATCH_ERROR;
					cur->tok_value = INVALID;
					return rc;
				}
				
			}
			else {
				if (dt_col_type != T_INT)
				{
					rc = DATATYPE_MISMATCH_ERROR;
					cur->tok_value = INVALID;
					return rc;
				}

			}
			
			// printf("cur_sym %s\n", cur_sym);
			// call delete here
			rc = update_records_in_table(new_entry, f_col_idx, cur_sym, cur->tok_string, s_col_idx, s_val,
				s_val_null_flag);
			return rc;
		}
		else 
		{
			// key words
			if (cur->tok_value != K_IS)
			{
				rc = INVALID_STATEMENT;
				cur->tok_value = INVALID;
				return rc;
			}

			cur = cur->next;

			if (cur->tok_value != K_NOT && cur->tok_value != K_NULL)
			{
				rc = INVALID_STATEMENT;
				cur->tok_value = INVALID;
				return rc;
			}

			char *cur_is_sym = "is";
			char *cur_is_not_sym = "isnot";
			
			int col_idx = f_col_idx;

			if (cur->tok_value == K_NOT)
			{
				cur = cur->next;
				if (cur->tok_value != K_NULL)
				{
					rc = INVALID_STATEMENT;
					cur->tok_value = INVALID;
					return rc;
				}

				// is not null
				rc = update_records_in_table(new_entry, f_col_idx, cur_is_not_sym,
				 cur->tok_string, s_col_idx, s_val, s_val_null_flag);
				return rc;

			}
			else {
				// is null
				rc = update_records_in_table(new_entry, f_col_idx, cur_is_sym,
				 cur->tok_string, s_col_idx, s_val, s_val_null_flag);
				return rc;	
			}

		}

		// get end of char
		cur = cur->next;
		if (cur->tok_value != EOC)
		{
			rc = INVALID_STATEMENT;
			cur->tok_value = INVALID;
			return rc;
		}

	}

	return rc;
}

int update_records_in_table(tpd_entry *new_entry, int col_idx, char *rel_sym, char *const_val, 
	int s_col, char *s_val, int null_flag)
{
	int rc = 0;
	// printf("col_idx %d\n", col_idx);
	// printf("rel_sym %s\n", rel_sym);
	// printf("const_val %s\n", const_val);


	cd_entry *col_arr = (cd_entry*)(((char *)new_entry) + new_entry->cd_offset);
	void *table_file_data = get_table_file_data(new_entry->table_name);

	table_file_header* tf_hdr = (table_file_header*)table_file_data;
	int rec_offset = tf_hdr->record_offset;
	int rec_size = tf_hdr->record_size;
	int num_cols = new_entry->num_columns;
	char *rec_mem = ((char *)table_file_data + rec_offset);
	
	char *cur_ptr = rec_mem;
	char *end_ptr = ((char *)table_file_data + tf_hdr->file_size);

	vector<void *> updated_rec_list;
	int update_count = 0;

	while (cur_ptr != end_ptr)
	{
		void *cur_rec_mem = calloc(1, rec_size);
		char *temp_ptr = cur_ptr;
		int idx = 0;
		bool update_flag = false;
		for (idx = 0; idx <= col_idx; idx++)
		{
			// get size
			int col_type = col_arr[idx].col_type;
			

			int field_size = (int)*(char *)temp_ptr;

			if (idx < col_idx)
			{
				temp_ptr = temp_ptr + field_size + 1;
				continue;
			}

			int rel_flag = -1;

			if (strcmp(rel_sym, "<") == 0)
			{
				rel_flag = 0;
			}
			else if (strcmp(rel_sym, ">") == 0) 
			{
				rel_flag = 1;
			}
			else if (strcmp(rel_sym, "=") == 0)
			{
				rel_flag = 2;
			}
			else if (strcmp(rel_sym, "is") == 0)
			{
				rel_flag = 3;
			}
			else if (strcmp(rel_sym, "isnot") == 0)
			{
				rel_flag = 4;
			}

			if (col_type == T_CHAR || col_type == T_VARCHAR)
			{
				char *str_size_char = (char*) temp_ptr;
				int str_size = (int)*temp_ptr;

				if (str_size == 0)
				{
					if (rel_flag == 3)
					{
						update_flag = true;
					}
					else if (rel_flag == 4)
					{
						update_flag = false;
					}
					else {
						update_flag = false;
					}

					temp_ptr = temp_ptr + field_size + 1;
					continue;
				}

				char *field_start_char = temp_ptr + 1;
				char field_val[str_size+1];

				memset(&field_val, '\0', str_size+1);
				memcpy(&field_val, field_start_char, str_size);
				field_val[str_size] = '\0';

				if (rel_flag == 2)
				{
					if (strcmp(field_val, const_val) == 0)
					{
						update_flag = true;
					}
					else
					{
						update_flag = false;
					}
				}
				else if (rel_flag == 0) {
					if (strcmp(field_val, const_val) < 0)
					{
						update_flag = true;
					}
					else
					{
						update_flag = false;
					}
				}
				else if (rel_flag == 1) {

					if (strcmp(field_val, const_val) > 0)
					{
						update_flag = true;
					}
					else
					{
						update_flag = false;
					}

				}
				else if (rel_flag == 3)
				{
					update_flag = false;
				}
				else if (rel_flag == 4)
				{
					update_flag = true;
				}

				temp_ptr = temp_ptr + field_size + 1;

			}
			else {

				char *int_field_size_char = (char *)temp_ptr;
				int int_field_size = (int)*int_field_size_char;

				if (int_field_size == 0)
				{
					if (rel_flag == 3)
					{
						update_flag = true;
					}
					else if (rel_flag == 4)
					{
						update_flag = false;
					}
					else {

						update_flag = false;
					}

					temp_ptr = temp_ptr + field_size + 1;
					continue;
				}

				int *field_val = (int *)(temp_ptr + 1);
				int int_const_val = atoi(const_val);

				// temp_ptr = temp_ptr + 1 + int_field_size;

				if (rel_flag == 0)
				{
					if (*field_val < int_const_val)
					{
						update_flag = true;
					}
					else {
						update_flag = false;
					}

				}
				else if (rel_flag == 1)
				{
					if (*field_val > int_const_val)
					{
						update_flag = true;
					}
					else {
						update_flag = false;
					}

				}
				else if (rel_flag == 2)
				{
					if (*field_val == int_const_val)
					{
						update_flag = true;
					}
					else {
						update_flag = false;
					}
				}
				else if (rel_flag == 3)
				{
					update_flag = false;
				}
				else if (rel_flag == 4)
				{
					update_flag = true;
				}

				temp_ptr = temp_ptr + field_size + 1;
			}
			
			
		}

		memcpy(cur_rec_mem, cur_ptr, rec_size);

		if (!update_flag)
		{
			updated_rec_list.push_back(cur_rec_mem);
		} else {

			// update the rec data
			char *temp_ptr = (char *)cur_ptr;
			char *aux_ptr = (char *)calloc(1, rec_size);
			char *aux_head = aux_ptr;

			int  jj = 0;

			for (jj = 0; jj < new_entry->num_columns; jj++)
			{
				// get size
				int col_type = col_arr[jj].col_type;
				int field_size = (int)*(char *)temp_ptr;

				if (jj != s_col)
				{
					// copy into aux mem
					memcpy(aux_ptr, temp_ptr, field_size + 1);
					temp_ptr = temp_ptr + field_size + 1;
					aux_ptr = aux_ptr + field_size + 1;
					continue;
				}

				if (col_type == T_CHAR || col_type == T_VARCHAR)
				{
					// size increase is causing overlap with next col data
					// need to update latest size post update

					// if null update only size
					if (null_flag)
					{
						int s_val_size = 0;
						char first_byte_val = (char)s_val_size;

						memcpy(aux_ptr, &first_byte_val, 1);
						aux_ptr = aux_ptr + 1;
						temp_ptr = temp_ptr + field_size + 1;
						continue;

					}

					int s_val_size = strlen(s_val);
					char first_byte_val = (char)s_val_size;

					memcpy(aux_ptr, &first_byte_val, 1);
					memcpy(aux_ptr + 1, s_val, s_val_size);

					aux_ptr = aux_ptr + s_val_size + 1;
					temp_ptr = temp_ptr + field_size + 1;
				}
				else {

					// if null update only size
					if (null_flag)
					{
						int s_val_size = 0;
						char first_byte_val = (char)s_val_size;

						memcpy(aux_ptr, &first_byte_val, 1);
						aux_ptr = aux_ptr + 1;
						temp_ptr = temp_ptr + field_size + 1;
						continue;

					}

					int s_val_size = 4;
					char first_byte_val = (char)s_val_size;
					int int_s_val = atoi(s_val);

					memcpy(aux_ptr, &first_byte_val, 1);
					memcpy(aux_ptr + 1, &int_s_val, s_val_size);

					aux_ptr = aux_ptr + s_val_size + 1;
					temp_ptr = temp_ptr + field_size + 1;
				}
			}

			memcpy(cur_rec_mem, aux_head, rec_size);
			updated_rec_list.push_back(cur_rec_mem);
			update_count++;
		}
		cur_ptr = cur_ptr + rec_size;
	}

	// printf("del_count %d\n", del_count);
	// update header file fields

	// int new_num_records = tf_hdr->num_records - del_count;
	// int new_file_size = tf_hdr->file_size - (del_count * rec_size);
	// tf_hdr->num_records = new_num_records;
	// tf_hdr->file_size = new_file_size;
	
	char file_table_name[MAX_IDENT_LEN+4];
	void *mem_ptr = NULL;
	FILE *fhandle = NULL;

	memset(file_table_name, 0, MAX_IDENT_LEN+4);
	strcpy(file_table_name, new_entry->table_name);
	strcat(file_table_name, ".tab");

	// write header followed by all the void * mem pointers one by one into file
	if((fhandle = fopen(file_table_name, "wbc")) == NULL) {
		rc = FILE_OPEN_ERROR;
		return rc;
	}
	else {
		fwrite((void *)tf_hdr, rec_offset, 1, fhandle);

		int ii = 0;

		for (ii = 0; ii < updated_rec_list.size(); ii++)
		{
			fwrite((void *)updated_rec_list[ii], rec_size, 1, fhandle);
		}

		fflush(fhandle);
		fclose(fhandle);
	}


	if (update_count == 0)
	{
		printf("Warning: 0 rows updated.\n\n");
	}
	else 
	{
		printf("Updated %d rows successfully\n\n", update_count);
	}

	printf("Table File Header (Latest):\n");
	printf(" file_size: %d\n", tf_hdr->file_size);
	printf(" record_offset: %d\n", tf_hdr->record_offset);
	printf(" num_records: %d\n", tf_hdr->num_records);
	printf(" record_size: %d\n\n", tf_hdr->record_size);

free_updated_rec_list:
	for (int c_itr = 0; c_itr < updated_rec_list.size(); c_itr++)
	{
		if (updated_rec_list[c_itr] != NULL)
			free(updated_rec_list[c_itr]);
	}
func_ret:
	return rc;
}

int initialize_tpd_list()
{
	int rc = 0;
	FILE *fhandle = NULL;
//	struct _stat file_stat;
	struct stat file_stat;

  /* Open for read */
  if((fhandle = fopen("dbfile.bin", "rbc")) == NULL)
	{
		if((fhandle = fopen("dbfile.bin", "wbc")) == NULL)
		{
			rc = FILE_OPEN_ERROR;
		}
    	else
		{
			g_tpd_list = NULL;
			g_tpd_list = (tpd_list*)calloc(1, sizeof(tpd_list));
			
			if (!g_tpd_list)
			{
				rc = MEMORY_ERROR;
			}
			else
			{
				g_tpd_list->list_size = sizeof(tpd_list);
				fwrite(g_tpd_list, sizeof(tpd_list), 1, fhandle);
				fflush(fhandle);
				fclose(fhandle);
			}
		}
	}
	else
	{
		/* There is a valid dbfile.bin file - get file size */
//		_fstat(_fileno(fhandle), &file_stat);
		fstat(fileno(fhandle), &file_stat);
		// printf("dbfile.bin size = %d\n", file_stat.st_size);

		g_tpd_list = (tpd_list*)calloc(1, file_stat.st_size);

		if (!g_tpd_list)
		{
			rc = MEMORY_ERROR;
		}
		else
		{
			fread(g_tpd_list, file_stat.st_size, 1, fhandle);
			fflush(fhandle);
			fclose(fhandle);

			if (g_tpd_list->list_size != file_stat.st_size)
			{
				rc = DBFILE_CORRUPTION;
			}

		}
	}
    
	return rc;
}
	
int add_tpd_to_list(tpd_entry *tpd)
{
	int rc = 0;
	int old_size = 0;
	FILE *fhandle = NULL;

	if((fhandle = fopen("dbfile.bin", "wbc")) == NULL)
	{
		rc = FILE_OPEN_ERROR;
	}
  	else
	{
		old_size = g_tpd_list->list_size;

		if (g_tpd_list->num_tables == 0)
		{
			/* If this is an empty list, overlap the dummy header */
			g_tpd_list->num_tables++;
		 	g_tpd_list->list_size += (tpd->tpd_size - sizeof(tpd_entry));
			fwrite(g_tpd_list, old_size - sizeof(tpd_entry), 1, fhandle);
		}
		else
		{
			/* There is at least 1, just append at the end */
			g_tpd_list->num_tables++;
		 	g_tpd_list->list_size += tpd->tpd_size;
			fwrite(g_tpd_list, old_size, 1, fhandle);
		}

		fwrite(tpd, tpd->tpd_size, 1, fhandle);
		fflush(fhandle);
		fclose(fhandle);
	}

	return rc;
}

int drop_tpd_from_list(char *tabname)
{
	int rc = 0;
	tpd_entry *cur = &(g_tpd_list->tpd_start);
	int num_tables = g_tpd_list->num_tables;
	bool found = false;
	int count = 0;

	if (num_tables > 0)
	{
		while ((!found) && (num_tables-- > 0))
		{
			if (strcasecmp(cur->table_name, tabname) == 0)
			{
				/* found it */
				found = true;
				int old_size = 0;
				FILE *fhandle = NULL;

				if((fhandle = fopen("dbfile.bin", "wbc")) == NULL)
				{
					rc = FILE_OPEN_ERROR;
				}
			  else
				{
					old_size = g_tpd_list->list_size;

					if (count == 0)
					{
						/* If this is the first entry */
						g_tpd_list->num_tables--;

						if (g_tpd_list->num_tables == 0)
						{
							/* This is the last table, null out dummy header */
							memset((void*)g_tpd_list, '\0', sizeof(tpd_list));
							g_tpd_list->list_size = sizeof(tpd_list);
							fwrite(g_tpd_list, sizeof(tpd_list), 1, fhandle);
						}
						else
						{
							/* First in list, but not the last one */
							g_tpd_list->list_size -= cur->tpd_size;

							/* First, write the 8 byte header */
							fwrite(g_tpd_list, sizeof(tpd_list) - sizeof(tpd_entry),
								     1, fhandle);

							/* Now write everything starting after the cur entry */
							fwrite((char*)cur + cur->tpd_size,
								     old_size - cur->tpd_size -
										 (sizeof(tpd_list) - sizeof(tpd_entry)),
								     1, fhandle);
						}
					}
					else
					{
						/* This is NOT the first entry - count > 0 */
						g_tpd_list->num_tables--;
					 	g_tpd_list->list_size -= cur->tpd_size;

						/* First, write everything from beginning to cur */
						fwrite(g_tpd_list, ((char*)cur - (char*)g_tpd_list),
									 1, fhandle);

						/* Check if cur is the last entry. Note that g_tdp_list->list_size
						   has already subtracted the cur->tpd_size, therefore it will
						   point to the start of cur if cur was the last entry */
						if ((char*)g_tpd_list + g_tpd_list->list_size == (char*)cur)
						{
							/* If true, nothing else to write */
						}
						else
						{
							/* NOT the last entry, copy everything from the beginning of the
							   next entry which is (cur + cur->tpd_size) and the remaining size */
							fwrite((char*)cur + cur->tpd_size,
										 old_size - cur->tpd_size -
										 ((char*)cur - (char*)g_tpd_list),							     
								     1, fhandle);
						}
					}

					fflush(fhandle);
					fclose(fhandle);
				}

				
			}
			else
			{
				if (num_tables > 0)
				{
					cur = (tpd_entry*)((char*)cur + cur->tpd_size);
					count++;
				}
			}
		}
	}
	
	if (!found)
	{
		rc = INVALID_TABLE_NAME;
	}

	return rc;
}

tpd_entry* get_tpd_from_list(char *tabname)
{
	tpd_entry *tpd = NULL;
	tpd_entry *cur = &(g_tpd_list->tpd_start);
	int num_tables = g_tpd_list->num_tables;
	bool found = false;

	if (num_tables > 0)
	{
		while ((!found) && (num_tables-- > 0))
		{
			if (strcasecmp(cur->table_name, tabname) == 0)
			{
				/* found it */
				found = true;
				tpd = cur;
			}
			else
			{
				if (num_tables > 0)
				{
					cur = (tpd_entry*)((char*)cur + cur->tpd_size);
				}
			}
		}
	}

	return tpd;
}
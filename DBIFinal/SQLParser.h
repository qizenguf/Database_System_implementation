#include <cstring>
#include <iostream>
#include <sstream>
#include "Schema.h"
#include "DBFile.h"
#include <stdio.h>
#include <stdlib.h> 
#include "Comparison.h"
using namespace std;

class SQLParser
{
private:
    string SQLCommand;
    char *tableName; 
    int numAtts;
    Attribute *myAtts;
    fType myType;
    OrderMaker *o;

public:
   SQLParser(string command)
    {
        SQLCommand=command;
        tableName=0;
        numAtts=0;
        myAtts=0;
        myType=heap;
        o = new OrderMaker;
    }

   void parse()
    {

        //The type of command is create file
        if(SQLCommand.find("CREATE TABLE")!=SQLCommand.npos)
        {
            int start_pos=SQLCommand.find("CREATE TABLE");            
            
            int lparenthe=SQLCommand.find("(");
            int rparenthe=SQLCommand.find(")");
            string table_str=SQLCommand.substr(start_pos+13,lparenthe-start_pos-13);
            int last_pos=table_str.find_last_not_of(" ");
            table_str=table_str.substr(0,last_pos+1);

            //get table name
            char table_name[20];
            memset(table_name,'\0',20);
            table_str.copy(table_name,table_str.length());
            tableName=table_name;
            
            //get all attributes
            string att_str=SQLCommand.substr(lparenthe+1,rparenthe-lparenthe);
            
            numAtts++;
            
            for(int i=0;i<att_str.length();i++)
            {
                if(att_str.at(i)==',')
                    numAtts++;
            }

            myAtts = new Attribute[numAtts];

            int start=0;
            
            for (int i=0;i<numAtts;i++)
            {
                
                int nextSeparater=start;
                if (att_str.find(",")!=att_str.npos)
                {
                    nextSeparater=att_str.find(",");
                    
                }
                 else if (att_str.find(")")!=att_str.npos)
                    nextSeparater=att_str.find(")");
    
                string att_type_str=att_str.substr(start,nextSeparater-start);
                
                int first_n_space=att_type_str.find_first_not_of(" ");
                int last_n_space=att_type_str.find_last_not_of(" ");
                att_type_str=att_type_str.substr(first_n_space,last_n_space-first_n_space+1);
                
                int space_pos=att_type_str.find(" ");
                string att_name=att_type_str.substr(0,space_pos);
                string att_type=att_type_str.substr(space_pos+1,att_type_str.length()-space_pos-1);
            
                char *attName = new char[20];
                memset(attName,'\0',20);
                att_name.copy(attName,att_name.length());
                
                myAtts[i].name=attName;
								cout<<i<<att_name<<" , "<<attName<<" , "<<myAtts[i].name<<endl;
                if(att_type.compare("INTEGER")==0)
                {                    
                    myAtts[i].myType=Int;
                }

                else if(att_type.compare("DOUBLE")==0)
                {
                    myAtts[i].myType=Double;
                }

                else if(att_type.compare("STRING")==0)
                {
                    myAtts[i].myType=String;
                }

                else {
                    cout << "Bad attribute type for " << myAtts[i].name << "\n";
                    exit (1);
                }

                start=nextSeparater+1;
                att_str=att_str.substr(start,att_str.length()-start);
                
                start=0;
                cout<<myAtts[0].name<<endl;
            }
            cout<<myAtts[0].name<<endl;
            //get file type
            int type_pos=SQLCommand.find("AS");
            string table_type_str=SQLCommand.substr(type_pos,SQLCommand.length()-type_pos);
            for(int t=0 ; t<numAtts; t++) cout<<t<<" : "<<myAtts[t].name<<endl;
            //handle heap file
            if(table_type_str.find("HEAP")!=table_type_str.npos)
            {
                myType=heap;

            }

            //handle sorted file
            else if (table_type_str.find("SORTED")!=table_type_str.npos)
            {
                myType=sorted;
                int start_pos=table_type_str.find("ON");
                int end_pos=table_type_str.find(";");
                string att_list=table_type_str.substr(start_pos+3,end_pos);
                cout<<att_list;

                int num_sorted_att=0;
                for(int i=0;i<att_list.length();i++)
                {
                    if(att_list.at(i)==','||att_list.at(i)==';')
                        num_sorted_att++;

                }

                int start=0;
                int nextSeparater=0;
                for(int t=0 ; t<numAtts; t++) cout<<t<<" : "<<myAtts[t].name<<endl;
                for(int i=0;i<num_sorted_att;i++)
                {
                    start=att_list.find_first_not_of(" ");
                    nextSeparater=start;
                    if (att_list.find(",")!=att_list.npos)
                    {
                        nextSeparater=att_list.find(",");
                    
                    }
                     else 
                        nextSeparater=att_list.find(";");

                    string att_name=att_list.substr(start,nextSeparater-start);
                    for(int t=0;t<numAtts;t++){
                    	if(att_name.compare(myAtts[t].name)==0){
                    		o->AddAttribute(t,myAtts[t].myType);
                    	cout<<att_name<<" : "<<myAtts[t].name<<endl;
                    	}
                    }
                    o->Print();
					//we need all att_name
                    cout<<att_name<<"\n";
                    att_list=att_list.substr(nextSeparater+1,att_list.length()-nextSeparater-1);
                }

            }
            else if (table_type_str.find("Tree")!=table_type_str.npos)
                myType=tree;

            else
                cout<<"Please indicate the right file type";

            //read the existing catalog file
            FILE *cata_file = fopen ("catalog", "r+");
            char str[80];
            
            long lSize=0;
            char * buffer;
            size_t result=0;

              // obtain file size:


            while (!feof(cata_file)) {
                 fgetc (cata_file);
                 lSize++;
             }

            rewind (cata_file);
            
              // allocate memory to contain the whole file:
            buffer = (char*) malloc (sizeof(char)*lSize);

            if (buffer == NULL) {fputs ("Memory error",stderr); exit (2);}

              // copy the file into the buffer:
            result = fread (buffer,1,lSize,cata_file);

            string str_buffer=buffer;

            str_buffer=str_buffer.substr(0,lSize-1);
            
            
            int pos=str_buffer.find(tableName);
            
            string tblPrefix="Tables/";
            string table_name_str=tblPrefix+tableName;
            table_name_str+=".bin";

            cout<<table_name_str<<endl;
            if(str_buffer.find(table_name_str)!=str_buffer.npos)
            {   
            	cout<<"Table already existed!\n";
            }
            else
            {
            //    fclose(cata_file);
            //    cata_file = fopen ("data/catalog", "a");
                fprintf(cata_file,"BEGIN\n");
                fprintf(cata_file,"%s\n",tableName);
                fprintf(cata_file,"Tables/%s.bin\n",tableName);
                for(int i=0;i<numAtts;i++)
                {
                    fprintf(cata_file,"%s",myAtts[i].name);
                    if(myAtts[i].myType==Int)                
                        fprintf(cata_file," Int\n");
                    else if (myAtts[i].myType==Double)
                        fprintf(cata_file," Double\n");
                    else
                        fprintf(cata_file," String\n");
                }

                fprintf(cata_file,"END\n");
                fprintf(cata_file,"\n");
                fclose(cata_file);
                DBFile _df;
                string path_str="Tables\\";
                path_str+=tableName;
                path_str+=".bin";
                char path[200];
                memset(path,'\0',200);
                path_str.copy(path,path_str.length());
                SortInfo *t= new SortInfo;
                t->om=o;
                t->runLength=120;
                _df.Create(path,myType,(void *)t);

                cout<<"Table created.\n";
            
            }

        }


        else if(SQLCommand.find("DROP TABLE")!=SQLCommand.npos)
        {
            int start=SQLCommand.find("DROP TABLE");
            int end=SQLCommand.find(";");
            string table_str=SQLCommand.substr(start,end-start);
            table_str=table_str.substr(0,table_str.find_last_not_of(" ")+1);
            table_str=table_str.substr(11,table_str.length()-11);
            

            char table_name[20];
            memset(table_name,'\0',20);
            table_str.copy(table_name,table_str.length());
            tableName=table_name;

            
            //remove the actual .bin file
            string datafile_path_str="Tables/";
            datafile_path_str+=tableName;
            datafile_path_str+=".bin";

            char datafile_path[50];
            memset(datafile_path,'\0',50);
            datafile_path_str.copy(datafile_path,datafile_path_str.length());    
						
            if( remove(datafile_path) != 0 ){
                cout<< "Table does not exist!\n" ;
          	}
            else
            {
								datafile_path_str+=".header";
            		memset(datafile_path,'\0',50);
            		datafile_path_str.copy(datafile_path,datafile_path_str.length());
            		remove(datafile_path);
                FILE *cata_file = fopen ("catalog", "r");
                char str[80];
                long lSize=0;
                char * buffer;
                size_t result=0;

                  // obtain file size:
                while (!feof(cata_file)) {
                     fgetc (cata_file);
                     lSize++;
                 }

                rewind (cata_file);
            
                  // allocate memory to contain the whole file:
                buffer = (char*) malloc (sizeof(char)*lSize);

                if (buffer == NULL) {fputs ("Memory error",stderr); exit (2);}

                  // copy the file into the buffer:
                result = fread (buffer,1,lSize,cata_file);

                string str_buffer=buffer;

                str_buffer=str_buffer.substr(0,lSize-1);
            
            
                int count=0;
                int begin_pos=0;
                int end_pos=0;
                string str_buf_substr=str_buffer;
                string table_name_str="Tables/";
                table_name_str+=tableName;
                table_name_str+=".bin";

                string content="\n";
                cout<<"here"<<endl;
                while(str_buf_substr.find("BEGIN\n")!=str_buf_substr.npos)
                {
                        begin_pos=str_buf_substr.find("BEGIN\n");
                        end_pos=str_buf_substr.find("END\n");
                        string sch_str=str_buf_substr.substr(begin_pos,end_pos+4-begin_pos);

                        if(sch_str.find(table_name_str)==sch_str.npos)
                        {                        
                            content+=sch_str;
                            content+="\n";
                        }
                        str_buf_substr=str_buf_substr.substr(end_pos+4,str_buf_substr.length()-end_pos-4);
                    
                    
                }

                fclose(cata_file);

                char *file_content=new char[5000];
                memset(file_content,'\0',5000);
                content.copy(file_content,content.length());
            
                cata_file=fopen("catalog", "w");
                fprintf(cata_file,"%s",file_content);
                fclose(cata_file);

                delete [] file_content;

                cout<<"Table dropped.\n";
            }
        }

        else if(SQLCommand.find("INSERT")!=SQLCommand.npos&&SQLCommand.find("INTO")!=SQLCommand.npos)
        {
            int start_pos=SQLCommand.find("INSERT")+7;
            int mid_pos=SQLCommand.find("INTO")-1;
            int end_pos=SQLCommand.find(";");
            string file_name=SQLCommand.substr(start_pos,mid_pos-start_pos);
            string table_str=SQLCommand.substr(mid_pos+6,end_pos-mid_pos-6);

            if(file_name.at(0)!='\''||file_name.at(file_name.length()-1)!='\'')
            {
                cout<<"Please give the correct command! use single quotes!";
            }

            else
            {
            file_name=file_name.substr(1,file_name.length()-2);
            

            char table_name[50];
            memset(table_name,'\0',50);
            table_str.copy(table_name,table_str.length());
            tableName=table_name;

            string table_name_str="Tables/";
            table_name_str+=tableName;
            table_name_str+=".bin";

            char path[200];
            memset(path,'\0',200);
            table_name_str.copy(path,table_name_str.length());

            cout<<path<<endl;

            FILE *sch_file=fopen(path,"r");
            if(sch_file==0)
            {
                cout<<"Insert error: table does not exist!";
            }
            
            
            DBFile _df;
            char loadMe[50];
            memset(loadMe,'\0',50);
            file_name.copy(loadMe,file_name.length());
            cout<<loadMe<<endl;

            Schema _sch("catalog",tableName);
						_df.Open(path);
            cout<<"num of atts:"<<_sch.GetNumAtts()<<endl;
            _df.Load(_sch,loadMe);
						_df.Close();
            }
        }



        else
        {
            cout<<"Please give the correct command!";
                
        }
    }
};
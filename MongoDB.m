classdef MongoDB < handle


    %   ----------------
    %   host_address - server hosting Mongo database.
    %   port         - port-numbers associated to server.
    %   dbname       - Mongo database name to connect to.
    %   user_name    - username required to authorize user.
    %   Password     - password required to authenticate a username.

    properties
        db_conn;
    end

    methods

        function obj  = MongoDB(mongo_setting)
            if nargin == 1
                host_address = mongo_setting.host_address ;
                port = str2double(mongo_setting.port);
                dbname = mongo_setting.dbname ;

                if isfield(mongo_setting, {'user_name'})
                    user_name = mongo_setting.user_name;% optional
                    password = mongo_setting.password;% optional
                    if isempty(user_name) || strcmp(user_name,"")
                        obj.db_conn = mongoc(host_address, port, dbname);
                    else
                        obj.db_conn = mongoc(host_address, port, dbname, "UserName", user_name, "Password", password);
                    end
                else
                    obj.db_conn = mongoc(host_address, port, dbname);
                end

            else
                host_address = "localhost";
                port = 27017;
                dbname = "admin";
                obj.db_conn = mongoc(host_address, port, dbname);
            end
        end


        function drop_collection(obj, collectname)
            dropCollection(obj.db_conn, collectname)

        end

        function create_col(obj, collectname, force_flag)

            if nargin<=2
                force_flag = 0;
            end

            flag_exist = 0;
            if ismember(collectname, obj.db_conn.CollectionNames)
                flag_exist = 1;
                if force_flag > 0
                    remove(obj.db_conn, collectname,'{}');
                end
            end

            if flag_exist == 0
                createCollection(obj.db_conn, collectname);
            end
        end


        function  [d_data, valid_conn] = get_from_col(obj, collectname, d_filter, selected_fields, sort_fields)
            % Get or read documents from a collection of database based on
            % a filter, selected fields and sort option

            % Input arguments:
            % ----------------
            %
            % COLLECTNAME - is the name of the collection in the MongoDB database
            % D_FILTER - is a structure array with a field-name and its start-stop values to get documents GTE start and LTE stop.
            % d_filter can also be a field-name with a list of indexes.
            % SELECTED_FIELDS - is the name of fields in each document that are desired to get
            % SORT_FIELDS - is an option for getting documents in sorted manner

            valid_conn = isopen(obj.db_conn);

            if nargin>= 3
                filter_cmd = create_filter(obj, d_filter);
            end

            if valid_conn
                if nargin== 2
                    d_data = find(obj.db_conn, collectname);

                elseif nargin == 3
                    d_data = find(obj.db_conn, collectname, Query=filter_cmd);

                elseif nargin>= 4
                    projection_cmd = '{';
                    for t=1:length(selected_fields)
                        if iscell(selected_fields)
                            selected_field = char(selected_fields{t});
                        else
                            selected_field = char(selected_fields(t));
                        end

                        if t<length(selected_fields)
                            projection_cmd = [projection_cmd,'"',selected_field,'":1.0,'];
                        else
                            projection_cmd = [projection_cmd,'"',selected_field,'":1.0}'];
                        end
                    end
                    if nargin == 4 && ~isempty(selected_fields)
                        d_data = find(obj.db_conn, collectname, Query=filter_cmd,  Projection=projection_cmd);
                    else
                        sortquery  = '{';
                        for t=1:length(sort_fields)
                            if t<length(sort_fields)
                                sortquery  = [sortquery ,'"',sort_fields{t},'":1.0,'];
                            else
                                sortquery  = [sortquery ,'"',sort_fields{t},'":1.0}'];
                            end
                        end
                        if ~isempty(selected_fields)
                            d_data = find(obj.db_conn, collectname, Query=filter_cmd, Projection=projection_cmd, Sort=sortquery);
                        else
                            d_data = find(obj.db_conn, collectname, Query=filter_cmd, Sort=sortquery);
                        end
                    end
                end

            else
                d_data = nan;
                warning("closed mongo db connection")
            end
        end

        function  [n, valid_conn] = insert_to_col( obj, collectname, documents)
            % Insert or write documents to a collection of database with the option of imposing custom encoder to
            %have a proper format for storing data in mongodb

            % Input arguments:
            % ----------------
            %
            % COLLECTNAME - is the name of the collection in the MongoDB database
            % DOCUMENTS   - MATLAB struct, struct array, table,

            % Ouputs:
            % ----------------
            % N - is number of inserted documents or NAN if there no
            % connection
            % VALID_CONN indicate the stutus of MongoDB connection


            if ~isfield(documents, 'update_time')
                documents = add_time(documents);
            end

            valid_conn = isopen(obj.db_conn);
            if valid_conn
                try
                    n = mongo_insert_modified(obj, obj.db_conn, collectname, documents);
                catch err
                    if contains(err.message, 'Unable to find collection')
                        create_col(obj, collectname, true)
                        n = mongo_insert_modified(obj, obj.db_conn, collectname, documents);
                    else
                        error(err.message)
                    end
                end
            else
                n = nan;
                warning("closed mongo db connection, please initial the connection")
            end

            % update_time local function
            function doc = add_time(doc)
                t = datetime('now','TimeZone','Asia/Tehran','Format','yyyyMMdd HH:mm:ss Z');

                for i = 1:length(doc)
                    doc(i).update_time = t;
                end
            end

            function insertCount = mongo_insert_modified(obj, mongodbconn,collectname,documents,varargin)

                % MONGO_INSERT_MODIFIED is a small modifed version of original MATLAB
                % insert function to handle NANs for fast get
                %
                % INSERTCOUNT = INSERT(MONGODBCONN,COLLECTNAME,DOCUMENTS)
                % INSERT dcouments in a collection
                %
                % Input arguments:
                % ----------------
                %
                % MONGODBCONN - Mongo database object
                % COLLECTNAME - Collection name
                % DOCUMENTS   - MATLAB struct, struct array, table,
                %               containers.Map object, character vector, string represeting
                %               data to be inserted to MongoDB.
                %
                % Example:
                % --------
                % insertcount = insert(mongodbconn,"product",'{"key1":"value1","key2":"value2"}')

                % Copyright 2021 The MathWorks, Inc.

                p = inputParser;

                p.addRequired("mongodbconn",@(x)validateattributes(x,"database.mongo.connection","scalar"));
                p.addRequired("collectname",@(x)validateattributes(x,["string","char"],{'scalartext'}));
                p.addRequired("documents",@(x)validateattributes(x,["string","char","cell","struct","table"],"nonempty"));
                p.addParameter("ByPassValidation",false,@(x)validateattributes(x,"logical","scalar"));
                p.addParameter("Validate",false,@(x)validateattributes(x,"logical","scalar"));

                p.parse(mongodbconn,collectname,documents,varargin{:});

                % Check if Mongo Database Object is valid
                if ~isopen(mongodbconn)
                    error(message('database:mongodb:InvalidConnection'));
                end

                collectname = char(p.Results.collectname);
                documents = p.Results.documents;

                switch class(documents)

                    case {'table'}

                        documents = table2struct(documents);

                    case {'string'}

                        if documents.strlength == 0
                            error(message('database:mongodb:ExpectedNonempty','''documents'''));
                        end

                        validateattributes(documents,{'string'},{'scalartext'});
                        documents = jsondecode(char(documents));

                    case {'char'}

                        validateattributes(documents,{'char'},{'scalartext'});
                        documents = jsondecode(char(documents));

                    case {'cell'}

                        if ~all(cellfun(@isstruct,documents))
                            error(message('database:mongodb:InsertError','cell array of structures'));
                        end

                    case {'struct'}

                        %do nothing

                    otherwise

                        %do nothing
                end

                bypassvalidation = p.Results.ByPassValidation;
                validate = p.Results.Validate;

                try
                    mongodbconn.ConnectionHandle.initializeBulkOperation(collectname);
                    if isstruct(documents)
                        for i = 1:numel(documents)
                            %mongodbconn.ConnectionHandle.createInsertBulkBatch(collectname,jsonencode(documents(i)),validate,bypassvalidation);
                            mongodbconn.ConnectionHandle.createInsertBulkBatch(collectname,jsonencode(documents(i), 'ConvertInfAndNaN', false),validate,bypassvalidation);
                        end
                    else
                        for i = 1:numel(documents)
                            %mongodbconn.ConnectionHandle.createInsertBulkBatch(collectname,jsonencode(documents{i}),validate,bypassvalidation);
                            mongodbconn.ConnectionHandle.createInsertBulkBatch(collectname,jsonencode(documents{i}, 'ConvertInfAndNaN', false),validate,bypassvalidation);
                        end
                    end
                    insertCount = mongodbconn.ConnectionHandle.executeBulkInsert();
                catch ME
                    throw(ME)
                end

            end




        end

        function num_doc = del_from_col(obj, collectname, d_filter, findquery)

            if nargin < 3
                %                 error('plz check num of inputs')
                findquery = '{}';
            elseif nargin == 3
                findquery = create_filter(obj, d_filter);
            end
            num_doc = remove(obj.db_conn, collectname, findquery);
        end

        function num_doc = update_doc(obj, collectname, findquery, updatequery)
            num_doc = update(obj.db_conn, collectname, findquery ,updatequery);
        end

        function close_db(obj)
            close(obj.db_conn);
        end


        % create_filter
        function filter_cmd = create_filter(obj, d_filter)
            if isa(d_filter, 'dictionary')
                filter_cmd = create_filter_dict(d_filter);

            elseif isstruct(d_filter)
                filter_cmd = create_filter_struct(d_filter);

            end

            function filter_cmd = create_filter_struct(d_filter)

                filter_cmd = '{';
                for t = 1:length(d_filter)
                    temp_fields = fieldnames(d_filter(t));

                    if ~isfield(d_filter(t), 'val_list') || isempty(d_filter(t).val_list) && (isfield(d_filter(t), 'start') && isfield(d_filter(t), 'stop'))
                        % handle for start-stop method
                        if t < length(d_filter)
                            if ismember({'start'}, temp_fields) && ismember({'stop'}, temp_fields)
                                filter_cmd = [filter_cmd,'"',d_filter(t).field,'":{"$gte":', num2str(d_filter(t).start), ',"$lte":', num2str(d_filter(t).stop), '},'];

                            elseif ismember({'start'}, temp_fields)
                                filter_cmd = [filter_cmd,'"',d_filter(t).field,'":{"$gte":', num2str(d_filter(t).start), '},'];

                            elseif  ismember({'stop'}, temp_fields)
                                filter_cmd = [filter_cmd,'"',d_filter(t).field,'":{"$lte":', num2str(d_filter(t).stop), '},'];
                            end

                        else
                            if ismember({'start'}, temp_fields) && ismember({'stop'}, temp_fields)
                                filter_cmd = [filter_cmd,'"',d_filter(t).field,'":{"$gte":', num2str(d_filter(t).start), ',"$lte":', num2str(d_filter(t).stop), '}}'];

                            elseif ismember({'start'}, temp_fields)
                                filter_cmd = [filter_cmd,'"',d_filter(t).field,'":{"$gte":', num2str(d_filter(t).start), '}}'];

                            elseif  ismember({'stop'}, temp_fields)
                                filter_cmd = [filter_cmd,'"',d_filter(t).field,'":{"$lte":', num2str(d_filter(t).stop), '}}'];
                            end
                        end

                    elseif isfield(d_filter(t), 'val_list') && ~isempty(d_filter(t).val_list)
                        % handle for list of index method
                        if isnumeric(d_filter(t).val_list)
                            val_list  = num2str(int64(d_filter(t).val_list));
                            max_val = numel(num2str(max(d_filter(t).val_list)));
                            for t1 = max_val+1:-1:2
                                temp = ' ';
                                for k = 2:t1
                                    temp = [temp, ' '];
                                end
                                %                         temp
                                val_list = strrep(val_list, temp, ' ');
                            end
                            val_list = strrep(val_list, ' ', ',');
                            if t < length(d_filter)
                                filter_cmd = [filter_cmd,'"',d_filter(t).field,'":{"$in":[', val_list, ']},'];
                            else
                                filter_cmd = [filter_cmd,'"',d_filter(t).field,'":{"$in":[', val_list, ']}}'];
                            end


                        else
                            %                         ["weights_buy_true", "weights_sell_true"]
                            val_list_temp  = d_filter(t).val_list;

                            val_list = [char(val_list_temp{1})];
                            for n = 2:length(val_list_temp)
                                val_list = [val_list '","' val_list_temp{n}];
                                %                             strcat(val_list,"","", val_list_temp(n))
                            end
                            %                         val_list = d_filter(t).val_list;

                            if t < length(d_filter)
                                filter_cmd = [filter_cmd,'"',d_filter(t).field,'":{"$in":["', val_list, '"]},'];
                            else
                                filter_cmd = [filter_cmd,'"',d_filter(t).field,'":{"$in":["', val_list, '"]}}'];
                            end


                        end

                    else
                        error('plz fix your selection method')
                    end
                end
            end

            function filter_cmd = create_filter2( d_filter)

                filter_cmd = '{';
                for t = 1:length(d_filter)

                    val_list  = num2str(int64(d_filter(t).val_list));

                    max_val = numel(num2str(max(d_filter(t).val_list)));
                    for t1 = max_val+1:-1:2
                        temp = ' ';
                        for k = 2:t1
                            temp = [temp, ' '];
                        end
                        val_list = strrep(val_list, temp, ' ');
                    end
                    val_list = strrep(val_list, ' ', ',');

                    if t < length(d_filter)
                        filter_cmd = [filter_cmd,'"',d_filter(t).field,'":{"$in":[', val_list, ']},'];
                    else
                        filter_cmd = [filter_cmd,'"',d_filter(t).field,'":{"$in":[', val_list, ']}}'];
                    end
                end

            end

            function filter_cmd = create_filter_dict(d_filter)

                keys_ = keys(d_filter);
                filter_cmd = '{';
                for t=1:length(keys_)

                    val_ = d_filter(keys_(t));
                    val_list  = num2str(int64(val_{1}));

                    max_val = numel(num2str(max(val_{1})));
                    for t1 = max_val+1:-1:2
                        temp = ' ';
                        for k = 2:t1
                            temp = [temp, ' '];
                        end
                        %                     temp
                        val_list = strrep(val_list, temp, ' ');
                    end
                    val_list = strrep(val_list, ' ', ',');

                    if t < length(keys_)
                        filter_cmd = [filter_cmd,'"',char(keys_(t)),'":{"$in":[', val_list, ']},'];
                    else
                        filter_cmd = [filter_cmd,'"',char(keys_(t)),'":{"$in":[', val_list, ']}}'];
                    end
                end
            end

        end


    end

end


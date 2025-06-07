#!/usr/bin/python2.7
#
# Interface for the assignement
#

import psycopg2
import psycopg2
import psycopg2.extras
import psycopg2.sql as sql
import io 


DATABASE_NAME = 'dds_assgn1'


def create_metadata_tables_if_not_exists(cursor):

    # For Round Robin Partitioning
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS round_robin_metadata (
            original_table_name VARCHAR(255) PRIMARY KEY,
            num_partitions INT NOT NULL,
            current_total_rows BIGINT NOT NULL
        );
    """)
    # For Range Partitioning
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS range_config_metadata (
            original_table_name VARCHAR(255) PRIMARY KEY,
            num_partitions INT NOT NULL,
            min_val FLOAT NOT NULL,
            max_val FLOAT NOT NULL
        );
    """)

def getopenconnection(user='postgres', password='1234', dbname='postgres'):
    return psycopg2.connect(        
        dbname=dbname,
        user=user,
        host='localhost',
        password=password
    )


def loadratings(ratingstablename, ratingsfilepath, openconnection): 
    """
    Function to load data in @ratingsfilepath file to a table called @ratingstablename.
    """
    main_table_name = ratingstablename # As per problem description
    main_table_identifier = sql.Identifier(main_table_name)
    
    cursor = openconnection.cursor()
    cursor.execute(
        sql.SQL("""
            CREATE TABLE IF NOT EXISTS {} (
                userid INT,
                movieid INT,
                rating FLOAT
            );
        """).format(main_table_identifier)
    )
    

    sio = io.StringIO()
    try:
        with open(ratingsfilepath, 'r') as f:
            for line in f:
                parts = line.strip().split('::')
                if len(parts) == 4: 
                    user_id, movie_id, rating, _ = parts[0], parts[1], parts[2], parts[3]
                    sio.write(f"{user_id}\t{movie_id}\t{rating}\n")
        sio.seek(0)

        copy_sql_query = sql.SQL("COPY {} (userid, movieid, rating) FROM STDIN WITH DELIMITER AS E'\\t'").format(main_table_identifier)
        cursor.copy_expert(sql=copy_sql_query, file=sio)
        
        openconnection.commit()

    except FileNotFoundError:
        print(f"Error: File not found at {ratingsfilepath}")
        openconnection.rollback()
    except Exception as e:
        print(f"Error loading data into '{main_table_name}': {e}")
        openconnection.rollback()

def rangepartition(ratingstablename, numberofpartitions, openconnection):
    """
    Function to create partitions of main table based on range of ratings.
    """
    con = openconnection
    cursor = con.cursor()

    RANGE_TABLE_PREFIX = 'range_part'
    
    create_metadata_tables_if_not_exists(cursor)
    
    if numberofpartitions <= 0:
        print("Error: Number of partitions (N) must be positive.")
        return

    min_rating_val = 0.0
    max_rating_val = 5.0
    step = (max_rating_val - min_rating_val) / numberofpartitions

    original_table_identifier = sql.Identifier(ratingstablename)

    for i in range(numberofpartitions):
        partition_name = RANGE_TABLE_PREFIX + str(i)
        partition_identifier = sql.Identifier(partition_name)

        cursor.execute(
            sql.SQL("""
                CREATE TABLE IF NOT EXISTS {} (
                    userid INT,
                    movieid INT,
                    rating FLOAT
                );
            """).format(partition_identifier)
        )
        cursor.execute(sql.SQL("TRUNCATE TABLE {};").format(partition_identifier))

        current_lower_bound = min_rating_val + i * step
        current_upper_bound = min_rating_val + (i + 1) * step

        if i == numberofpartitions - 1: 
            current_upper_bound = max_rating_val
        
        if numberofpartitions == 1:
            condition_sql = sql.SQL("rating >= %s AND rating <= %s")
            params = (min_rating_val, max_rating_val)
        elif i == 0: 
            condition_sql = sql.SQL("rating >= %s AND rating <= %s")
            params = (current_lower_bound, current_upper_bound)
        else: 
            condition_sql = sql.SQL("rating > %s AND rating <= %s")
            params = (current_lower_bound, current_upper_bound)

        populate_sql = sql.SQL("""
            INSERT INTO {} (userid, movieid, rating)
            SELECT userid, movieid, rating FROM {}
            WHERE {};
        """).format(partition_identifier, original_table_identifier, condition_sql)
        
        cursor.execute(populate_sql, params)

    meta_table_identifier = sql.Identifier("range_config_metadata")

    upsert_meta_sql = sql.SQL("""
        INSERT INTO {} (original_table_name, num_partitions, min_val, max_val)
        VALUES (%s, %s, %s, %s)
        ON CONFLICT (original_table_name) DO UPDATE SET
            num_partitions = EXCLUDED.num_partitions,
            min_val = EXCLUDED.min_val,
            max_val = EXCLUDED.max_val;
    """).format(meta_table_identifier)
    cursor.execute(upsert_meta_sql, (ratingstablename, numberofpartitions, min_rating_val, max_rating_val))
    
    con.commit()


def roundrobinpartition(ratingstablename, numberofpartitions, openconnection):
    """
    Function to create partitions of main table using round robin approach.
    """
    con = openconnection
    cursor = con.cursor()
    RROBIN_TABLE_PREFIX = 'rrobin_part'
    
    create_metadata_tables_if_not_exists(cursor)
    
    if numberofpartitions <= 0:
        print("Error: Number of partitions (N) must be positive.")
        return

    original_table_identifier = sql.Identifier(ratingstablename)
    partition_tables_data = [ [] for _ in range(numberofpartitions)]

    for i in range(numberofpartitions):
        partition_name = RROBIN_TABLE_PREFIX + str(i)
        partition_identifier = sql.Identifier(partition_name)
        cursor.execute(
            sql.SQL("""
                CREATE TABLE IF NOT EXISTS {} (
                    userid INT,
                    movieid INT,
                    rating FLOAT
                );
            """).format(partition_identifier)
        )
        cursor.execute(sql.SQL("TRUNCATE TABLE {};").format(partition_identifier))

    cursor.execute(sql.SQL("SELECT userid, movieid, rating FROM {};").format(original_table_identifier))
    all_rows = cursor.fetchall()

    if not all_rows:
        # print(f"No data in '{ratingstablename}' to partition.")
        pass
    else:
        for row_index, row_data in enumerate(all_rows):
            target_partition_index = row_index % numberofpartitions
            partition_tables_data[target_partition_index].append(row_data)

        for i in range(numberofpartitions):
            if partition_tables_data[i]:
                partition_name = RROBIN_TABLE_PREFIX + str(i)
                partition_identifier = sql.Identifier(partition_name)
                
                insert_query = sql.SQL("INSERT INTO {} (userid, movieid, rating) VALUES %s;").format(partition_identifier)
                psycopg2.extras.execute_values(
                    cursor,
                    insert_query.as_string(cursor.connection),
                    partition_tables_data[i],
                    page_size=1000
                )
    
    meta_table_identifier = sql.Identifier("round_robin_metadata")

    total_rows_distributed = len(all_rows)
    upsert_meta_sql = sql.SQL("""
        INSERT INTO {} (original_table_name, num_partitions, current_total_rows)
        VALUES (%s, %s, %s)
        ON CONFLICT (original_table_name) DO UPDATE SET
            num_partitions = EXCLUDED.num_partitions,
            current_total_rows = EXCLUDED.current_total_rows;
    """).format(meta_table_identifier)
    cursor.execute(upsert_meta_sql, (ratingstablename, numberofpartitions, total_rows_distributed))

    con.commit()


def roundrobininsert(ratingstablename, userid, itemid, rating, openconnection):
    """
    Function to insert a new row into the main table and specific partition based on round robin
    approach.
    """
    con = openconnection
    cursor = con.cursor()
    RROBIN_TABLE_PREFIX = 'rrobin_part'
    create_metadata_tables_if_not_exists(cursor)

    original_table_identifier = sql.Identifier(ratingstablename)
    meta_table_identifier = sql.Identifier("round_robin_metadata")
    
    movieid = itemid 

    insert_main_sql = sql.SQL("INSERT INTO {} (userid, movieid, rating) VALUES (%s, %s, %s);").format(original_table_identifier)
    cursor.execute(insert_main_sql, (userid, movieid, rating))

    select_meta_sql = sql.SQL("SELECT num_partitions, current_total_rows FROM {} WHERE original_table_name = %s;").format(meta_table_identifier)
    cursor.execute(select_meta_sql, (ratingstablename,))
    meta_data = cursor.fetchone()

    if not meta_data:
        print(f"Error: Metadata for round-robin partitioning of '{ratingstablename}' not found. Run RoundRobin_Partition first.")
        con.rollback()
        return

    num_partitions, current_total_rows = meta_data
    if num_partitions <= 0:
        print(f"Error: Invalid number of partitions ({num_partitions}) in metadata for '{ratingstablename}'.")
        con.rollback()
        return
        
    target_partition_index = int(current_total_rows % num_partitions)
    target_partition_name = RROBIN_TABLE_PREFIX + str(target_partition_index)
    target_partition_identifier = sql.Identifier(target_partition_name)

    insert_partition_sql = sql.SQL("INSERT INTO {} (userid, movieid, rating) VALUES (%s, %s, %s);").format(target_partition_identifier)
    cursor.execute(insert_partition_sql, (userid, movieid, rating))

    update_meta_sql = sql.SQL("UPDATE {} SET current_total_rows = current_total_rows + 1 WHERE original_table_name = %s;").format(meta_table_identifier)
    cursor.execute(update_meta_sql, (ratingstablename,))
    
    con.commit()
    

def rangeinsert(ratingstablename, userid, itemid, rating, openconnection):
    """
    Function to insert a new row into the main table and specific partition based on range rating.
    """
    con = openconnection
    cursor = con.cursor()

    
    original_table_identifier = sql.Identifier(ratingstablename)
    meta_config_table_identifier = sql.Identifier("range_config_metadata")

    movieid = itemid

    insert_main_sql = sql.SQL("INSERT INTO {} (userid, movieid, rating) VALUES (%s, %s, %s);").format(original_table_identifier)
    cursor.execute(insert_main_sql, (userid, movieid, rating))

    select_meta_sql = sql.SQL("SELECT num_partitions, min_val, max_val FROM {} WHERE original_table_name = %s;").format(meta_config_table_identifier)
    cursor.execute(select_meta_sql, (ratingstablename,))
    meta_data = cursor.fetchone()

    if not meta_data:
        print(f"Error: Metadata for range partitioning of '{ratingstablename}' not found. Run Range_Partition first.")
        con.rollback()
        return

    N, min_val, max_val = meta_data
    if N <= 0:
        print(f"Error: Invalid number of partitions ({N}) in metadata for '{ratingstablename}'.")
        con.rollback()
        return

    if not (min_val <= rating <= max_val):
        con.commit()
        return

    step = (max_val - min_val) / N
    target_partition_name = None

    for i in range(N):
        current_lower_bound = min_val + i * step
        current_upper_bound = min_val + (i + 1) * step
        if i == N - 1:
            current_upper_bound = max_val

        is_in_partition = False
        if N == 1:
            if rating >= min_val and rating <= max_val:
                is_in_partition = True
        elif i == 0:
            if rating >= current_lower_bound and rating <= current_upper_bound:
                is_in_partition = True
        else: 
            if rating > current_lower_bound and rating <= current_upper_bound:
                is_in_partition = True
        
        if is_in_partition:
            target_partition_name = f"range_part{i}"
            break
    
    if target_partition_name:
        target_partition_identifier = sql.Identifier(target_partition_name)
        insert_partition_sql = sql.SQL("INSERT INTO {} (userid, movieid, rating) VALUES (%s, %s, %s);").format(target_partition_identifier)
        cursor.execute(insert_partition_sql, (userid, movieid, rating))
        
    else:
        pass

    con.commit()

def create_db(dbname):
    """
    We create a DB by connecting to the default user and database of Postgres
    The function first checks if an existing database exists for a given name, else creates it.
    :return:None
    """
    # Connect to the default database
    con = getopenconnection(dbname='postgres')
    con.set_isolation_level(psycopg2.extensions.ISOLATION_LEVEL_AUTOCOMMIT)
    cur = con.cursor()

    # Check if an existing database with the same name exists
    cur.execute('SELECT COUNT(*) FROM pg_catalog.pg_database WHERE datname=\'%s\'' % (dbname,))
    count = cur.fetchone()[0]
    if count == 0:
        cur.execute('CREATE DATABASE %s' % (dbname,))  # Create the database
    else:
        print('A database named {0} already exists'.format(dbname))

    # Clean up
    cur.close()
    con.close()

def count_partitions(prefix, openconnection):
    """
    Function to count the number of tables which have the @prefix in their name somewhere.
    """
    con = openconnection
    cur = con.cursor()
    cur.execute("select count(*) from pg_stat_user_tables where relname like " + "'" + prefix + "%';")
    count = cur.fetchone()[0]
    cur.close()

    return count

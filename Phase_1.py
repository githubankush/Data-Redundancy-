import re
import mysql.connector
from mysql.connector import Error
import time
start_time = time.perf_counter()

def split_alphanumeric(word):
    letters = ''.join(filter(str.isalpha, word))
    numbers = ''.join(filter(str.isdigit, word))
    first_type = "letter" if word and word[0].isalpha() else "number" if word else "unknown"
    return letters, numbers, first_type


def is_number(string):
    try:
        float(string)
        return True
    except ValueError:
        return False


def process_info(medicine_info, key_std_pair):
    words = medicine_info.split()
    power_types = ['MG', 'GM', 'ML']
    pack_types = list(key_std_pair.keys())
    pack_name = []
    power = ''
    power_type = ''
    pack_type = ''
    pack_size = ''
    other = ''

    i = 0

    # Loop for pack_name
    while i < len(words):
        word = words[i]
        # print("Word: ", word)

        if i == 0 and word.isdigit():
            pack_name.append(word)
            i += 1
            continue
        elif i == 0 and any(char.isdigit() for char in word):
            pack_name.append(word)
            i += 1
            continue
        elif any(char.isdigit() for char in word):
            break
        elif word.isdigit():
            break
        elif word in pack_types:
            pack_type = key_std_pair[word]

            i += 1
        elif word in power_types:
            power_type = word
            i += 1
        else:
            if pack_type == '' or pack_size == '' or power_type == '' or power == '':
                pack_name.append(word)
                i += 1

    # Loop for all other fields
    while i < len(words):
        next_word = words[i]
        if is_number(next_word):
            if power == '' and pack_type == '' and power_type == '':
                power = next_word
                i += 1
            else:
                pack_size += ' ' + next_word
                i += 1
        elif any(char.isdigit() for char in next_word):
            letters, numbers, first_type = split_alphanumeric(next_word)
            if first_type == "number":
                if re.match(r'\d+[Xx*]\d+', next_word):
                    pack_size = next_word
                    i += 1
                    continue
                if letters in power_types:
                    power_type = letters
                    power = numbers
                    i += 1
                elif letters in pack_types:
                    pack_type = key_std_pair[letters]
                    if power == '':
                        power = numbers
                    else:
                        pack_size = numbers
                    i += 1
                else:
                    pack_name.append(next_word)
                    i += 1
                    continue
            elif first_type != "number":
                if letters in power_types:
                    power_type = letters
                    pack_size = numbers
                    i += 1
                elif letters in pack_types:
                    pack_type = key_std_pair[letters]
                    pack_size = numbers
                    i += 1
                else:
                    pack_name.append(next_word)
                    i += 1
                    continue

        elif next_word in power_types:
            power_type = next_word
            i += 1

        elif next_word in pack_types:
            pack_type = key_std_pair[next_word]
            i += 1

        elif power == '' and power_type == '' and pack_size == '' and pack_type == '':
            pack_name.append(next_word)
            i += 1
            continue

        else:
            other += ' ' + next_word
            i += 1

    pack_name = ' '.join(pack_name).strip()

    info = {
        'pack_name': pack_name,
        'power': power,
        'power_type': power_type,
        'pack_size': pack_size,
        'pack_type': pack_type,
        'other': other,
    }

    return info


def map_strings(string1, string2, key_std_pair):
    info1 = process_info(string1, key_std_pair)
    info2 = process_info(string2, key_std_pair)


    if (info1['pack_name'] == info2['pack_name'] and
            info1['power'] == info2['power'] and
            info1['pack_type'] == info2['pack_type'] and info1['other'] == info2['other']):

        if info1['power_type'] and info2['power_type']:
            if info1['power_type'] != info2['power_type']:
                return string1, string2, 'No'
            else:
                return string1, string2, 'Yes'
        else:
            return string1, string2, 'Yes'

    return string1, string2, 'No'



def process_products():
    try:
        connection = mysql.connector.connect(
            host="localhost",
            user="root",
            password="",
            database='listapp_db',
            port=3306
        )
        cur = connection.cursor()

        select_keyword_std = "SELECT keyword, standard_name FROM m16j_keyword"
        cur.execute(select_keyword_std)
        collections = cur.fetchall()
        key_std_pair = {
            i[0]: i[1] for i in collections
        }

        join_query = f"""
            SELECT m16j_demo_poducts_1.product_id, m16j_demo_poducts_1.product_name, m16j_demo_poducts_1.company_name, 
                   m16j_demo_poducts_1.drug_name, m16j_demo_poducts_1.form, m16j_demo_poducts_1.pack_size, 
                   m16j_demo_poducts_1.packing_type, m16j_demo_poducts_1.mrp, m16j_demo_poducts_1.rate, 
                   m16j_demo_poducts_1.schedule, m16j_demo_poducts_1.status, m16j_demo_poducts_1.is_draft, 
                   m16j_demo_poducts_1.point_to, m16j_demo_poducts_1.denotion, m16j_demo_poducts_1.add_date, 
                   m16j_demo_poducts_1.rflag,c.*
                   FROM m16j_demo_poducts_1
                    INNER JOIN c ON m16j_demo_poducts_1.company_name = c.company_id
                    WHERE m16j_demo_poducts_1.denotion = '' 
                   
                    
            """

        cur.execute(join_query)
        all_records = cur.fetchall()

        for all_record in all_records:
            current_product = all_record
            product_id = current_product[0]
            product_name = current_product[1]
            company_name = current_product[17]
            company_name_start_letter = company_name.split()[0].upper()

            query = """
                SELECT * FROM m16j_demo_poducts_1
                WHERE product_id = %s AND denotion = ''
            """
            cur.execute(query, (product_id,))
            result = cur.fetchall()
            # print(result)
            if not result:
                continue

            info = process_info(product_name, key_std_pair)
            Pack_name = info['pack_name']
            search_and_join_query = f"""
             SELECT m16j_demo_poducts_1.product_id, m16j_demo_poducts_1.product_name, m16j_demo_poducts_1.company_name, 
                   m16j_demo_poducts_1.drug_name, m16j_demo_poducts_1.form, m16j_demo_poducts_1.pack_size, 
                   m16j_demo_poducts_1.packing_type, m16j_demo_poducts_1.mrp, m16j_demo_poducts_1.rate, 
                   m16j_demo_poducts_1.schedule, m16j_demo_poducts_1.status, m16j_demo_poducts_1.is_draft, 
                   m16j_demo_poducts_1.point_to, m16j_demo_poducts_1.denotion, m16j_demo_poducts_1.add_date, 
                   m16j_demo_poducts_1.rflag,c.*
            FROM m16j_demo_poducts_1
            INNER JOIN c ON m16j_demo_poducts_1.company_name = c.company_id
            WHERE m16j_demo_poducts_1.product_name LIKE %s
            AND m16j_demo_poducts_1.product_id != %s
            """

            cur.execute(search_and_join_query, (f"%{Pack_name}%", product_id))
            similar_records = cur.fetchall()
            exact_map = []

            for similar_record in similar_records:
                similar_product = similar_record
                similar_product_name = similar_record[1]
                cmp_name = similar_record[17]
                cmp_name_start_letter = cmp_name.split()[0].upper()
                result = map_strings(product_name, similar_product_name, key_std_pair)
                if result[2] == 'Yes':
                    if company_name_start_letter == cmp_name_start_letter:
                        exact_map.append(similar_record)

            
            flagg = 0
            s_product = ""
            d_product = ""
            s_flag = 0
            d_flag = 0

            for i, j in enumerate(exact_map):
                if j[13] == 'd':
                    q1 = """SELECT * FROM m16j_demo_poducts_1
                            WHERE product_id = %s"""
                    cur.execute(q1, (j[12],))
                    rest = cur.fetchall()
                    if rest is not None:
                        d_product = rest[0]

                    flagg = 1
                    d_flag = 1

                elif j[13] == 's':
                    s_flag = 1
                    s_product = j
                    flagg = 1

            if s_flag != 0:
                update_query_duplicate = """
                                       UPDATE m16j_demo_poducts_1
                                       SET point_to = %s, denotion = 'd', status = 0, rflag = 1
                                       WHERE product_id = %s
                                   """
                cur.execute(update_query_duplicate, (s_product[0], product_id))

                insert_analytics_query = f"""
                               INSERT INTO analytics_table1(std_id,duplicate_id,std_name, duplicate_name,main_id,count)
                               VALUES ( %s, %s,%s, %s, %s, %s)
                               """
                cur.execute(insert_analytics_query,
                            (s_product[0], product_id, s_product[1], product_name, product_id, 1))

            elif d_flag != 0:
                update_query_duplicate = """
                                       UPDATE m16j_demo_poducts_1
                                       SET point_to = %s, denotion = 'd', status = 0, rflag = 1
                                       WHERE product_id = %s
                                   """
                cur.execute(update_query_duplicate, (d_product[0], product_id))

                insert_analytics_query = f"""
                                       INSERT INTO analytics_table1(std_id,duplicate_id,std_name, duplicate_name,main_id,count)
                                       VALUES ( %s, %s,%s, %s, %s, %s)
                                       """
                cur.execute(insert_analytics_query,
                            (d_product[0], product_id, d_product[1], product_name, product_id, 1))

            keyword_available = False
            keyword_is_standard = False

            if flagg == 0:
                for word in product_name.split():

                    if word in key_std_pair:
                        keyword_available = True
                        if key_std_pair[word] == word:
                            keyword_is_standard = True

                flag = 0

                if keyword_available and not keyword_is_standard:
                    for i, similar_record in enumerate(exact_map):
                        if flag == 0:
                            temp_similar_record = similar_record
                            temp_product_name = temp_similar_record[1]
                            for wordd in temp_product_name.split():
                                if wordd in key_std_pair and key_std_pair[wordd] == wordd:
                                    temp1 = current_product
                                    current_product = similar_record
                                    exact_map[i] = temp1
                                    flag = 1
                                    break

                for i in exact_map:
                    update_query_duplicate = """
                                               UPDATE m16j_demo_poducts_1
                                               SET point_to = %s, denotion = 'd', status = 0, rflag = 1
                                               WHERE product_id = %s
                                            """
                    cur.execute(update_query_duplicate, (current_product[0], i[0]))

                    insert_analytics_query = f"""
                       INSERT INTO analytics_table1(std_id,duplicate_id,std_name, duplicate_name,main_id,count)
                       VALUES ( %s, %s,%s, %s, %s, %s)
                       """
                    cur.execute(insert_analytics_query,
                                (current_product[0], i[0], current_product[1], i[1], product_id, len(exact_map)))

                update_query = """
                            UPDATE m16j_demo_poducts_1
                            SET point_to = %s, denotion = 's', status = 1, rflag = 1
                            WHERE product_id = %s
                        """
                cur.execute(update_query, (current_product[0], current_product[0]))

                insert_analytics_query = f"""
                            INSERT INTO analytics_table1(std_id,duplicate_id,std_name,duplicate_name,main_id,count)
                                      VALUES ( %s, %s,%s, %s, %s, %s)
                        """
                cur.execute(insert_analytics_query,
                            (current_product[0], current_product[0], current_product[1], current_product[1], product_id,1))

        connection.commit()
        end_time = time.perf_counter()
        elapsed_time = end_time - start_time
        print(f"Elapsed time: {elapsed_time:.1f} seconds")
    except Error as e:
        print(f"Error: {e}")
    finally:
        if connection.is_connected():
            cur.close()
            connection.close()

def main():
    process_products()

if __name__ == "__main__":
    main()

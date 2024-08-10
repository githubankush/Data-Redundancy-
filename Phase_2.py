import mysql.connector
from mysql.connector import Error

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

        join_query = """
           SELECT m16j_demo_poducts_1.product_id, m16j_demo_poducts_1.product_name, m16j_demo_poducts_1.company_name, 
                   m16j_demo_poducts_1.drug_name, m16j_demo_poducts_1.form, m16j_demo_poducts_1.pack_size, 
                   m16j_demo_poducts_1.packing_type, m16j_demo_poducts_1.mrp, m16j_demo_poducts_1.rate, 
                   m16j_demo_poducts_1.schedule, m16j_demo_poducts_1.status, m16j_demo_poducts_1.is_draft, 
                   m16j_demo_poducts_1.point_to, m16j_demo_poducts_1.denotion, m16j_demo_poducts_1.add_date, 
                   m16j_demo_poducts_1.rflag, c.*
                   FROM m16j_demo_poducts_1
                   INNER JOIN c ON m16j_demo_poducts_1.company_name = c.company_id
           WHERE denotion = 's' 
        """

        cur.execute(join_query)
        all_records = cur.fetchall()

        for all_record in all_records:
            current_product = all_record
            product_id = current_product[0]
            product_name = current_product[1]
            company_name = current_product[17]

            search_and_join_query = """
               SELECT m16j_demo_poducts_1.product_id, m16j_demo_poducts_1.product_name, m16j_demo_poducts_1.company_name, 
                   m16j_demo_poducts_1.drug_name, m16j_demo_poducts_1.form, m16j_demo_poducts_1.pack_size, 
                   m16j_demo_poducts_1.packing_type, m16j_demo_poducts_1.mrp, m16j_demo_poducts_1.rate, 
                   m16j_demo_poducts_1.schedule, m16j_demo_poducts_1.status, m16j_demo_poducts_1.is_draft, 
                   m16j_demo_poducts_1.point_to, m16j_demo_poducts_1.denotion, m16j_demo_poducts_1.add_date, 
                   m16j_demo_poducts_1.rflag, c.*
                FROM m16j_demo_poducts_1
                INNER JOIN c ON m16j_demo_poducts_1.company_name = c.company_id
                WHERE point_to = %s AND denotion = 'd'
            """
            cur.execute(search_and_join_query, (product_id,))
            similar_records = cur.fetchall()
            exact_map = similar_records

            if '(' in company_name and ')' in company_name:
                for i, similar_record in enumerate(exact_map):
                    if not '(' in similar_record[17] and not ')' in similar_record[17]:
                        temp1 = current_product
                        current_product = similar_record
                        exact_map[i] = temp1
                        break

            drugs = set()
            forms = set()
            pack_sizes = set()
            pack_types = set()
            mrps = set()
            schedules = set()

            for i in exact_map:
                drugs.add(i[3])
                forms.add(i[4])
                pack_sizes.add(i[5])
                pack_types.add(i[6])
                mrps.add(i[7])
                schedules.add(i[9])

            drugs.add(current_product[3])
            forms.add(current_product[4])
            pack_sizes.add(current_product[5])
            pack_types.add(current_product[6])
            mrps.add(current_product[7])
            schedules.add(current_product[9])

            def process_entries(entries):
                return ",".join(sorted(filter(None, map(str, entries))))

            drug = process_entries(drugs)
            form = process_entries(forms)
            pack_size = process_entries(pack_sizes)
            pack_type = process_entries(pack_types)
            mrp = process_entries(mrps)
            schedule = process_entries(schedules)

            if product_id != current_product[0]:
                for i in exact_map:
                    update_query_duplicate = """
                            UPDATE m16j_demo_poducts_1
                            SET point_to = %s, denotion = 'd', status = 0, rflag = 2
                            WHERE product_id = %s
                        """
                    cur.execute(update_query_duplicate, (current_product[0], i[0]))

                    insert_analytics_query = """
                            INSERT INTO analytics_table2(std_id, duplicate_id, std_name, duplicate_name, main_id, count)
                            VALUES (%s, %s, %s, %s, %s, %s)
                        """
                    cur.execute(insert_analytics_query,
                                (current_product[0], i[0], current_product[1], i[1], product_id, len(exact_map)))

            update_query = """
                UPDATE m16j_demo_poducts_1
                SET point_to = %s, denotion = 's', status = 1, rflag = 2,
                    drug_name = %s, form = %s, pack_size = %s, packing_type = %s, mrp = %s, schedule = %s
                WHERE product_id = %s
            """
            cur.execute(update_query, (current_product[0], drug, form, pack_size,
                                       pack_type, mrp, schedule, current_product[0]))

            insert_analytics_query = """
                INSERT INTO analytics_table2(std_id, duplicate_id, std_name, duplicate_name, main_id, count)
                VALUES (%s, %s, %s, %s, %s, %s)
            """
            cur.execute(insert_analytics_query, (
                current_product[0], current_product[0], current_product[1], current_product[1], product_id, 1))

        connection.commit()

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

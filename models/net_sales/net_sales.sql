{{ 
	config(
        post_hook=[
            """
                INSERT INTO {{ this }} (id, move_line_id, date, invoice_origin, name, move_type, gl_account_id, gl_account_name, credit, debit, product_id_0, product_id_1, price_unit, quantity, company_id_0, company_id_1, producer, tdv_code, warehouse, partner_id, partner_name, category_id, sale_origin_0, sale_origin_1, is_mto, mto_product_id, picking_date, sales_order_date) 
                    SELECT id, move_line_id, date, invoice_origin, name, move_type, gl_account_id, gl_account_name, credit, debit, product_id_0, product_id_1, price_unit, quantity, company_id_0, company_id_1, producer, tdv_code, warehouse, partner_id, partner_name, category_id, sale_origin_0, sale_origin_1, is_mto, mto_product_id, picking_date, sales_order_date FROM {{ ref('vt_receivable_vt_reversal_lines') }}
            """,
            """
                UPDATE
                    {{ this }} t1
                SET 
                    sales_order_date = t2.date_order
                FROM
                    {{ source('odooerp', 'sale_order') }} t2
                WHERE
                    t1.sale_origin_1 = t2.name AND t1.company_id_0 = t2.company_id_0 AND sales_order_date IS NULL
            """,
            """
                UPDATE
                    {{ this }} t1
                SET 
                    picking_date = t2.date_done
                FROM
                    {{ source('odooerp', 'stock_picking') }} t2
                WHERE
                    t1.sale_origin_1 = t2.origin AND t1.company_id_0 = t2.company_id_0 AND picking_date IS NULL
                    AND t2.picking_type_code = 'outgoing'
            """,
            """
                DELETE FROM {{ this }} WHERE move_line_id IN (SELECT move_line_id FROM {{ ref('deleted_move_line_ids') }})
            """,
            """
                INSERT INTO {{ this }} (gl_account_name, gl_account_id, partner_name, company_id_1, company_id_0, product_id_1, warehouse, credit, debit, id, move_line_id, date, invoice_origin, name, move_type, product_id_0, price_unit, quantity, producer, tdv_code, partner_id, category_id, sale_origin_0, sale_origin_1, is_mto, mto_product_id, picking_date, sales_order_date)
                    SELECT * FROM {{ ref('cogs_producers') }}
            """,
            """
                INSERT INTO {{ this }} (company_id_1, company_id_0, gl_account_name, gl_account_id, product_id_1, warehouse, credit, debit, id, move_line_id, date, invoice_origin, name, move_type, product_id_0, price_unit, quantity, producer, partner_id, partner_name, category_id, is_mto, mto_product_id)
                    SELECT * FROM {{ ref('cogs_producers2') }}
            """,
            """
                INSERT INTO {{ this }} (company_id_1, gl_account_name, warehouse, credit, debit, id, move_line_id, date, invoice_origin, name, move_type, gl_account_id, price_unit, quantity, company_id_0, producer, partner_id, partner_name, category_id, is_mto, mto_product_id)
                    SELECT * FROM {{ ref('sales_producers') }}
            """,
            """
                INSERT INTO {{ this }} (company_id_1, gl_account_name, warehouse, credit, debit, id, move_line_id, date, invoice_origin, name, move_type, gl_account_id, product_id_0, price_unit, quantity, company_id_0, producer, is_mto)
                    SELECT * FROM {{ ref('sales_producers2') }}
            """,
            """
                INSERT INTO {{ this }} (gl_account_name, company_id_1, product_id_1, warehouse, credit, debit, id, move_line_id, date, invoice_origin, name, move_type, gl_account_id, product_id_0, price_unit, quantity, company_id_0, producer, tdv_code, partner_id, partner_name, category_id, sale_origin_0, sale_origin_1, is_mto, mto_product_id, picking_date, sales_order_date)
                    SELECT * FROM {{ ref('cogs_producers3') }}
            """,
            """
                INSERT INTO {{ this }} (gl_account_name, company_id_1, product_id_1, warehouse, credit, debit, id, move_line_id, date, invoice_origin, name, move_type, gl_account_id, product_id_0, price_unit, quantity, company_id_0, producer, tdv_code, partner_id, partner_name, category_id, sale_origin_0, sale_origin_1, is_mto, mto_product_id, picking_date, sales_order_date)
                    SELECT * FROM {{ ref('sales_producers3') }}
            """
        ],
		materialized='table'
	) 
}}

SELECT * FROM {{ ref('_net_sales') }}
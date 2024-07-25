{{ 
	config(
        pre_hook="""
                    INSERT INTO td_models_stg._net_sales (id, move_line_id, date, invoice_origin, name, move_type, gl_account_id, gl_account_name, credit, debit, product_id_0, product_id_1, price_unit, quantity, company_id_0, company_id_1, producer, tdv_code, warehouse, partner_id, partner_name, category_id, sale_origin_0, sale_origin_1, is_mto, mto_product_id, picking_date, sales_order_date) 
                        SELECT id, move_line_id, date, invoice_origin, name, move_type, gl_account_id, gl_account_name, credit, debit, product_id_0, product_id_1, price_unit, quantity, company_id_0, company_id_1, producer, tdv_code, warehouse, partner_id, partner_name, category_id, sale_origin_0, sale_origin_1, is_mto, mto_product_id, picking_date, sales_order_date FROM {{ ref('vt_driver_vt_mto_journals') }}
        """,
		materialized='table' 
	) 
}}

WITH mto_no_cogs AS (
    /* MTO invoices with no COGS */
    SELECT 
        id,
        CONCAT('[', CONCAT(id, CONCAT(', ', CONCAT(CHR(39), CONCAT(name, CONCAT(CHR(39), ']')))))) AS journal_origin,
        (CASE 
            WHEN CAN_JSON_PARSE(REPLACE(sale_origin_id, CHR(39), '"')) THEN 
                json_extract_array_element_text(REPLACE(sale_origin_id, CHR(39), '"'), 1)
            ELSE
                NULL
        END) AS sale_origin
    FROM
        {{ source('odooerp', 'account_move') }}
    WHERE
        state = 'posted' AND is_mto = 'True' AND sale_origin_id IS NOT NULL ANd sale_origin_id <> 'False'
        AND id NOT IN (SELECT DISTINCT move_id_0 FROM {{ source('odooerp', 'account_move_line') }} WHERE account_id_1 = '500000 Cost of Goods' AND parent_state = 'posted')
),
journals_from_mto AS (
    /* Journals used to debit sales on behalf of the driver */
    SELECT
        id, name, date,
        json_extract_array_element_text(REPLACE(origin, CHR(39), '"'), 0) AS mto_origin_id, 
        json_extract_array_element_text(REPLACE(origin, CHR(39), '"'), 1) AS mto_origin_name
    FROM
        {{ source('odooerp', 'account_move') }}
    WHERE
        origin IN (SELECT journal_origin FROM mto_no_cogs) AND state = 'posted'
),
vt_reversal_lines_for_journals_from_mto AS (
    SELECT
        id,
        -1 AS move_line_id,
        date,
        invoice_origin,
        name,
        move_type,
        gl_account_id,
        gl_account_name,
        debit AS credit, /* This is what causes the actual reversal */
        credit AS debit, /* This is what causes the actual reversal */
        product_id_0,
        product_id_1,
        price_unit,
        quantity,
        company_id_0,
        company_id_1,
        producer,
        tdv_code,
        warehouse,
        partner_id,
        partner_name,
        category_id,
        sale_origin_0,
        sale_origin_1,
        is_mto,
        mto_product_id,
        NULL AS picking_date,
        NULL AS sales_order_date
    FROM
        td_models_stg._net_sales
    WHERE
        id IN (SELECT id FROM journals_from_mto)
),
driver_inv_from_mto_inv AS (
    SELECT DISTINCT
        id,
        invoice_origin,
        partner_id,
        partner_name,
        category_id,
        warehouse
    FROM
        td_models_stg._net_sales
    WHERE
        is_mto != 'True' AND move_type = 'out_invoice' AND category_id NOT LIKE '%13%'
        AND invoice_origin IN (SELECT sale_origin FROM mto_no_cogs)
),
vt_receivable_credit_lines AS (
    SELECT
        journals_from_mto.id,
        -1 AS move_line_id,
        journals_from_mto.date,
        net_sales.invoice_origin,
        journals_from_mto.name,
        'entry' AS move_type,
        gl_account_id,
        gl_account_name,
        debit AS credit,
        credit AS debit,
        mto_product_id::SMALLINT AS product_id_0,
        product_product.display_name AS product_id_1,
        price_unit,
        quantity,
        net_sales.company_id_0,
        net_sales.company_id_1,
        COALESCE(product_product.producer_1, 'No Producer') AS producer,
        tdv_code,
        driver_inv_from_mto_inv.warehouse,
        driver_inv_from_mto_inv.partner_id,
        driver_inv_from_mto_inv.partner_name,
        driver_inv_from_mto_inv.category_id,
        NULL AS sale_origin_0,
        NULL AS sale_origin_1,
        'False' AS is_mto,
        NULL AS mto_product_id,
        CAST(NULL AS TIMESTAMP) AS picking_date,
        CAST(NULL AS TIMESTAMP) AS sales_order_date
    FROM
        td_models_stg._net_sales net_sales LEFT OUTER JOIN {{ source('odooerp', 'product_product') }}
            ON mto_product_id = product_product.id

        LEFT JOIN driver_inv_from_mto_inv
            ON net_sales.sale_origin_1 = driver_inv_from_mto_inv.invoice_origin
        
        LEFT JOIN journals_from_mto
            ON net_sales.id = journals_from_mto.mto_origin_id
    WHERE
        net_sales.id IN (SELECT mto_origin_id FROM journals_from_mto)
        AND net_sales.gl_account_name = '400000 Sales'
)
SELECT * FROM vt_receivable_credit_lines
UNION ALL
SELECT * FROM vt_reversal_lines_for_journals_from_mto

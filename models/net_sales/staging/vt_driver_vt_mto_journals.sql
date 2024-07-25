-- depends_on: {{ ref('_net_sales') }}

{{ 
	config( 
        pre_hook=[
            """
                UPDATE
                    td_models_stg._net_sales t1
                SET 
                    producer = t2.Producer
                FROM
                    {{ source('odoo_etl', 'v_sku_producers') }} t2 
                WHERE
                    t1.tdv_code = t2.code AND (t1.producer IS NULL OR t1.producer = 'False' OR t1.producer = '' OR t1.producer = 'No Producer')
            """, 
            """
                UPDATE
                    td_models_stg._net_sales t1
                SET 
                    warehouse = t2.warehouse_id_1
                FROM
                    {{ source('odooerp', 'sale_order') }} t2 
                WHERE
                    t1.sale_origin_1 = t2.name AND t1.is_mto = 'True';
            """
        ],
		materialized='table' 
	) 
}}

WITH mto_no_cogs AS (
    /* MTO invoices with no COGS */
    SELECT 
        id,
        sale_origin_id,
        (CASE 
            WHEN CAN_JSON_PARSE(REPLACE(sale_origin_id, CHR(39), '"')) THEN 
                json_extract_array_element_text(REPLACE(sale_origin_id, CHR(39), '"'), 1)
            ELSE
                NULL
        END) AS sale_origin
    FROM
        {{ source('odooerp', 'account_move') }}
    WHERE
        state = 'posted' AND is_mto = 'True' AND sale_origin_id IS NOT NULL AND sale_origin_id <> 'False'
        AND id NOT IN (SELECT DISTINCT move_id_0 FROM {{ source('odooerp', 'account_move_line') }} WHERE account_id_1 = '500000 Cost of Goods' AND parent_state = 'posted')
),
driver_inv_for_mto AS (
    SELECT
        *
    FROM
        td_models_stg._net_sales
    WHERE
        gl_account_name = '500000 Cost of Goods' AND move_type = 'out_invoice' AND debit > 0
        AND invoice_origin IN (SELECT sale_origin FROM mto_no_cogs)
),
cogs_per_sku_per_inv AS (
    SELECT
        product_id_0,
        id AS move_id,
        invoice_origin,
        warehouse,
        SUM(debit) AS total_cogs,
        SUM(quantity) AS total_qty,
        SUM(debit) / SUM(quantity) AS unit_cogs
    FROM
        driver_inv_for_mto
    GROUP BY
        product_id_0,
        id,
        invoice_origin,
        warehouse
),
vt_mto_cogs AS (
    SELECT
        a.id AS mto_move_id,
        a.sale_origin_0,
        a.sale_origin_1,
        a.mto_product_id,
        a.name,
        a.company_id_0,
        a.company_id_1,
        a.partner_id,
        a.partner_name,
        a.category_id,
        a.producer,
        b.warehouse,
        MIN(a.date) AS date,
        SUM(quantity) AS quantity,
        (SUM(quantity) * SUM(b.unit_cogs)) AS mto_cogs
    FROM
        td_models_stg._net_sales a LEFT JOIN cogs_per_sku_per_inv b
            ON a.sale_origin_1 = b.invoice_origin AND a.mto_product_id = b.product_id_0
    WHERE
        a.move_type = 'out_invoice' AND a.gl_account_name = '400000 Sales'
        AND a.id IN (SELECT id FROM mto_no_cogs)
    GROUP BY
        a.id,
        a.mto_product_id,
        a.sale_origin_0,
        a.sale_origin_1,
        a.name,
        a.company_id_0,
        a.company_id_1,
        a.partner_id,
        a.partner_name,
        a.category_id,
        a.producer,
        b.warehouse
),
vt_mto_journals AS (
    SELECT
        a.mto_move_id AS id,
        -1 AS move_line_id,
        a.date,
        NULL AS invoice_origin,
        a.name,
        'out_invoice' AS move_type,
        NULL AS gl_account_id,
        '500000 Cost of Goods' AS gl_account_name,
        0 AS credit,
        a.mto_cogs AS debit,
        NULL AS product_id_0,
        NULL AS product_id_1,
        0 AS price_unit,
        a.quantity,
        a.company_id_0,
        a.company_id_1,
        a.producer,
        NULL AS tdv_code,
        a.warehouse,
        a.partner_id,
        a.partner_name,
        a.category_id,
        a.sale_origin_0,
        a.sale_origin_1,
        'True' AS is_mto,
        a.mto_product_id,
        CAST(NULL AS TIMESTAMP) AS picking_date,
        CAST(NULL AS TIMESTAMP) AS sales_order_date
    FROM
        vt_mto_cogs a
),
vt_driver_journals_tmp AS (
    SELECT
        id,
        -1 AS move_line_id,
        date,
        invoice_origin,
        name,
        move_type,
        gl_account_id,
        gl_account_name,
        0 AS credit,
        0 AS debit,
        product_id_0,
        product_id_1,
        0 AS price_unit,
        SUM(quantity) AS quantity,
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
        mto_product_id
    FROM
        driver_inv_for_mto
    GROUP BY
        id,        
        date,
        invoice_origin,
        name,
        move_type,
        gl_account_id,
        gl_account_name,        
        product_id_0,
        product_id_1,
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
        mto_product_id
),
vt_driver_journals AS (
    SELECT
        a.id,
        a.move_line_id,
        a.date,
        a.invoice_origin,
        a.name,
        a.move_type,
        a.gl_account_id,
        a.gl_account_name,
        b.mto_cogs AS credit,
        a.debit,
        a.product_id_0,
        a.product_id_1,
        a.price_unit,
        a.quantity,
        a.company_id_0,
        a.company_id_1,
        a.producer,
        a.tdv_code,
        a.warehouse,
        a.partner_id,
        a.partner_name,
        a.category_id,
        a.sale_origin_0,
        a.sale_origin_1,
        a.is_mto,
        a.mto_product_id,
        NULL AS picking_date,
        NULL AS sales_order_date
    FROM
        vt_driver_journals_tmp a LEFT JOIN vt_mto_cogs b
            ON a.invoice_origin = b.sale_origin_1 AND a.product_id_0 = b.mto_product_id
)
SELECT * FROM vt_driver_journals
UNION ALL
SELECT * FROM vt_mto_journals
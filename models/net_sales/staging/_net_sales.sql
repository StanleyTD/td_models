{{ 
	config( 
		materialized='table'
	) 
}}


WITH _net_sales AS (
    SELECT 
        account_move.id,
        account_move_line.id AS move_line_id,
        account_move_line.date,
        account_move.invoice_origin,
        account_move.name,
        account_move.move_type,
        account_move_line.account_id_0 AS gl_account_id,
        account_account.display_name AS gl_account_name,
        account_move_line.credit,
        account_move_line.debit,
        account_move_line.product_id_0,
        account_move_line.product_id_1,
        account_move_line.price_unit,
        account_move_line.quantity,
        account_move_line.company_id_0,
        account_move_line.company_id_1,
        COALESCE(product_product.producer_1, 'No Producer') AS producer,
        product_product.code AS tdv_code,
        COALESCE(
            NULLIF(sale_order.warehouse_id_1, 'False'),
            NULLIF( json_extract_array_element_text( REPLACE(account_move_line.analytic_account_id, CHR(39), '"'), 1, true ), '' ),
            'Unknown Warehouse'
        ) AS warehouse,
        account_move_line.partner_id_0 AS partner_id,
        account_move_line.partner_id_1 AS partner_name,
        res_partner.category_id,
        (CASE 
            WHEN CAN_JSON_PARSE(REPLACE(account_move.sale_origin_id, CHR(39), '"')) THEN 
                json_extract_array_element_text(REPLACE(account_move.sale_origin_id, CHR(39), '"'), 0)
            ELSE
                NULL
        END) AS sale_origin_0,
        (CASE 
            WHEN CAN_JSON_PARSE(REPLACE(account_move.sale_origin_id, CHR(39), '"')) THEN 
                json_extract_array_element_text(REPLACE(account_move.sale_origin_id, CHR(39), '"'), 1)
            ELSE
                NULL
        END) AS sale_origin_1,
        account_move.is_mto,
        account_move_line.mto_product_id,
        stock_picking.date_done AS picking_date,
        sale_order.date_order AS sales_order_date

    FROM 
        {{ source('odooerp', 'account_move_line') }} LEFT OUTER JOIN {{ source('odooerp', 'account_move') }}
            ON account_move_line.move_id_0 = account_move.id
        
        LEFT OUTER JOIN {{ source('odooerp', 'product_product') }}
            ON account_move_line.product_id_0 = product_product.id OR account_move_line.mto_product_id = product_product.id
        
        LEFT OUTER JOIN {{ source('odooerp', 'account_account') }}
            ON account_move_line.account_id_0 = account_account.id

        LEFT OUTER JOIN {{ source('odooerp', 'sale_order') }}
            ON split_part(account_move.invoice_origin, ',', 1) = sale_order.name

        LEFT OUTER JOIN {{ source('odooerp', 'res_partner') }}
            ON account_move_line.partner_id_0 = res_partner.id
        
        LEFT OUTER JOIN {{ source('odooerp', 'stock_picking') }}
            ON sale_order.name = stock_picking.origin AND sale_order.company_id_0 = stock_picking.company_id_0
                AND stock_picking.picking_type_code = 'outgoing' AND stock_picking.state = 'done'
    WHERE
        account_move_line.parent_state = 'posted'
        AND account_move_line.account_internal_group IN ('income', 'expense')
        AND DATEDIFF(d, account_move_line.date, current_date) < 191
)
SELECT * FROM _net_sales
{{ 
	config(
        pre_hook=[
            """
                INSERT INTO td_models_stg._net_sales (id, move_line_id, date, invoice_origin, name, move_type, gl_account_id, gl_account_name, credit, debit, product_id_0, product_id_1, price_unit, quantity, company_id_0, company_id_1, producer, tdv_code, warehouse, partner_id, partner_name, category_id, sale_origin_0, sale_origin_1, is_mto, mto_product_id, picking_date, sales_order_date) 
                    SELECT id, move_line_id, date, invoice_origin, name, move_type, gl_account_id, gl_account_name, credit, debit, product_id_0, product_id_1, price_unit, quantity, company_id_0, company_id_1, producer, tdv_code, warehouse, partner_id, partner_name, category_id, sale_origin_0, sale_origin_1, is_mto, mto_product_id, picking_date, sales_order_date FROM {{ ref('vt_receivable_vt_reversal_lines') }}
            """,
            """
                UPDATE
                    td_models_stg._net_sales t1
                SET 
                    sales_order_date = t2.date_order
                FROM
                    {{ source('odooerp', 'sale_order') }} t2
                WHERE
                    t1.sale_origin_1 = t2.name AND t1.company_id_0 = t2.company_id_0 AND sales_order_date IS NULL
            """,
            """
                UPDATE
                    td_models_stg._net_sales t1
                SET 
                    picking_date = t2.date_done
                FROM
                    {{ source('odooerp', 'stock_picking') }} t2
                WHERE
                    t1.sale_origin_1 = t2.origin AND t1.company_id_0 = t2.company_id_0 AND picking_date IS NULL
                    AND t2.picking_type_code = 'outgoing'
            """,
            """
                DELETE FROM td_models_stg._net_sales WHERE move_line_id IN (28678780, 28949581, 29278675, 29278875, 29279884, 29315663, 29529739, 29535923, 29535927, 29535945, 29535951, 29535965, 29537295,
                    29537303, 29537323, 29537331, 29537341, 29537354, 29537358, 29537362, 29537368, 29537370, 29537380, 29537456, 29537464, 29537472, 29537479, 29537483, 29537485, 29537489, 29537499,
                    29686438, 29777344, 29891187, 29891649, 29891652, 29891709, 29891713, 29891719, 29891745, 29891759, 29902739, 30035545, 30158635, 30158636, 30158637, 30158638, 30158639, 30158640,
                    30158641, 30218472, 30218512, 30218645, 30218655, 30243153, 30243165, 30243179)
            """,
            """
                INSERT INTO td_models_stg._net_sales (gl_account_name, gl_account_id, partner_name, company_id_1, company_id_0, product_id_1, warehouse, credit, debit, id, move_line_id, date, invoice_origin, name, move_type, product_id_0, price_unit, quantity, producer, tdv_code, partner_id, category_id, sale_origin_0, sale_origin_1, is_mto, mto_product_id, picking_date, sales_order_date)
                    SELECT * FROM {{ ref('cogs_producers') }}
            """,
            """
                DELETE FROM td_models_stg._net_sales WHERE move_line_id IN (28689427, 28701924, 28705541, 28710503, 28743498, 28754532, 28761283, 28765874, 28774500, 28777838, 
                    28800723, 28815138, 28829497, 28837311, 28841443, 28845693, 28864885, 28878380, 28939608, 28949464, 28970601, 29001089, 29001448, 29049128, 29052111, 
                    29056208, 29058537, 29058546, 29064794, 29064810, 29113438, 29129829, 29131249, 29133525, 29139621, 29171646, 29246950, 29270808, 29276337, 29276835,
                    29283937, 29285859, 29289260, 29295446, 29311694, 29312241, 29313938, 29329219, 29330510, 29352925, 29373166, 29379060, 29391799, 29402676, 29406138, 
                    29430698, 29430835, 29454672, 29470983, 29473953, 29479861, 29503139, 29593433, 29596158, 29608263, 29617487, 29617646, 29617693, 29618071, 29686362, 
                    29701742, 29702481, 29702483, 29702529, 29703031, 29736144, 29750541, 29763662, 29767353, 29774903, 29774916, 29777343, 29791637, 29800848, 29801070, 
                    29801122, 29827574, 29849851, 29854888, 29892345, 29951875, 29977090, 29993748, 30013100, 30064149, 30079298, 30125887, 30135914, 30138645, 30174319, 
                    30176427, 30180674, 30181962, 30190875, 30191278, 30192341, 30197932, 30201864, 30202426, 30213715, 30226128, 30226488, 30233344, 30253328, 30260721, 
                    30270912, 30270951)
            """,
            """
                INSERT INTO td_models_stg._net_sales (company_id_1, company_id_0, gl_account_name, gl_account_id, product_id_1, warehouse, credit, debit, id, move_line_id, date, invoice_origin, name, move_type, product_id_0, price_unit, quantity, producer, partner_id, partner_name, category_id, is_mto, mto_product_id)
                    SELECT * FROM {{ ref('cogs_producers2') }}
            """,
            """
                DELETE FROM td_models_stg._net_sales WHERE move_line_id IN (28774848, 28687928, 29656815, 29777341, 29286651, 29286637, 29286616, 29286612, 29286639, 29286614, 30222538, 30222536, 30222528, 30222507, 28857722)
            """,
            """
                INSERT INTO td_models_stg._net_sales (company_id_1, gl_account_name, warehouse, credit, debit, id, move_line_id, date, invoice_origin, name, move_type, gl_account_id, price_unit, quantity, company_id_0, producer, partner_id, partner_name, category_id, is_mto, mto_product_id)
                    SELECT * FROM {{ ref('sales_producers') }}
            """,
            """
                DELETE FROM td_models_stg._net_sales WHERE move_line_id IN (28889127, 29078525, 29078915, 29121239, 29347276, 29454794, 29518228, 29796676, 29817334, 30093401, 30146990)
            """,
            """
                INSERT INTO td_models_stg._net_sales (company_id_1, gl_account_name, warehouse, credit, debit, id, move_line_id, date, invoice_origin, name, move_type, gl_account_id, product_id_0, price_unit, quantity, company_id_0, producer, is_mto)
                    SELECT * FROM {{ ref('sales_producers2') }}
            """,
            """
                DELETE FROM td_models_stg._net_sales WHERE move_line_id IN (30589975, 30542151, 30447609, 30239731, 30237064, 30231189, 30250771, 30302861, 30301464, 30330395, 30250704,
                    30233333, 30236946, 30303184, 30303735, 30294283, 30333976, 30335942, 30334069, 30356344, 30302707, 30330860, 30250685, 30401497, 30414188, 30416337, 30408986,
                    30423071, 30272558, 30426950, 30426973, 30362843, 30457117, 30421003, 30447845, 30516771, 30423651, 30423701, 30448508, 30465382, 30294205, 30451175, 30437924, 
                    30447660, 30466663, 30489346, 30430361, 30479635, 30480322, 30526216, 30531056, 30507309, 30237794, 30236936, 30276080, 30305271, 30290013, 30294060, 30258203,
                    30250739, 30364539, 30237116, 30265139, 30305235, 30290019, 30296089, 30328293, 30362550, 30366529, 30436747, 30396467, 30419098, 30419624, 30456037, 30482992,
                    30476518, 30330706, 30344229, 30494184, 30512166, 30510098, 30463815, 30463887, 30546691, 30449609, 30515508, 30378082, 30525590, 30582765, 30559852, 30590037,
                    30594944, 30624960, 30594467)
            """,
            """
                INSERT INTO td_models_stg._net_sales (gl_account_name, company_id_1, product_id_1, warehouse, credit, debit, id, move_line_id, date, invoice_origin, name, move_type, gl_account_id, product_id_0, price_unit, quantity, company_id_0, producer, tdv_code, partner_id, partner_name, category_id, sale_origin_0, sale_origin_1, is_mto, mto_product_id, picking_date, sales_order_date)
                    SELECT * FROM {{ ref('cogs_producers3') }}
            """,
            """
                DELETE FROM td_models_stg._net_sales WHERE move_line_id IN (30547478,  30487145, 30547445, 30457107)
            """,
            """
                INSERT INTO td_models_stg._net_sales (gl_account_name, company_id_1, product_id_1, warehouse, credit, debit, id, move_line_id, date, invoice_origin, name, move_type, gl_account_id, product_id_0, price_unit, quantity, company_id_0, producer, tdv_code, partner_id, partner_name, category_id, sale_origin_0, sale_origin_1, is_mto, mto_product_id, picking_date, sales_order_date)
                    SELECT * FROM {{ ref('sales_producers3') }}
            """
        ],
		materialized='table'
	) 
}}

SELECT * FROM td_models_stg._net_sales
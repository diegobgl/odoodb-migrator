import pgdb
import json 
import datetime
from sqlescapy import sqlescape

class data_dicts():
    data_12 = {}
    data_13 = {}

data_object =  data_dicts()

hostname = 'localhost'
username = 'justo'
password = 'justo'
database = 'tas_12'
database13 = 'tas_13'


global tablas_distintas 
tablas_distintas = [
    'iap_account','ir_attachment', 'res_users',
    'res_partner', 'product_product', 'mail_compose_message',
    'product_pricelist_item', 'crm_stage', 'calendar_event',
    'mail_activity', 'payment_acquirer','res_partner',
    'mail_channel_partner','mail_message_res_partner_needaction_rel',
    'product_template','sale_order_option','account_journal',
    'crm_team'
]
global columnas

def migrate_table_differ(con12, con13, table_name):
    cur12 = con12.cursor()
    cur12.execute( "SELECT column_name, data_type FROM information_schema.columns where table_name = '%s'" % table_name )

    cur13 = con13.cursor()
    cur13.execute( "SELECT column_name, data_type FROM information_schema.columns where table_name = '%s'" % table_name )

    columns_13 = ''
    for data in cur13.fetchall():
        columns_13 += data.column_name + ','
    columns_13 = columns_13[:-1]
    
    columns_12 = ''
    for data in cur12.fetchall():
        columns_12 += data.column_name + ','
    columns_12 = columns_12[:-1]
    
    cur12 = con12.cursor()

    if table_name in ['iap_account','ir_attachment', 'mail_compose_message','sale_order_option']:
        cur12.execute( "SELECT %s FROM %s ;" % (columns_13, table_name) )
    
    elif table_name in ['product_product']:
        columns_13 = columns_13.replace('combination_indices,','')
        columns_13 = columns_13.replace('can_image_variant_1024_be_zoomed,','')
        cur12.execute( "SELECT %s FROM %s ;" % (columns_13, table_name) )
    elif table_name in ['product_template']:
        columns_13 = columns_13.replace('can_image_1024_be_zoomed,','')
        columns_13 = columns_13.replace('has_configurable_attributes,','')
        cur12.execute( "SELECT %s FROM %s ;" % (columns_13, table_name) )
    elif table_name == 'product_pricelist_item':
        columns_13 = '"id","product_tmpl_id","product_id","categ_id","min_quantity","applied_on","base","base_pricelist_id","pricelist_id","price_surcharge","price_discount","price_round","price_min_margin","price_max_margin","company_id","currency_id","date_start","date_end","compute_price","fixed_price","percent_price","create_uid","create_date","write_uid","write_date"'
    elif table_name in ['res_users', 'calendar_event','mail_channel_partner'] :
        cur12.execute( "SELECT %s FROM %s ;" % (columns_12, table_name) )
    elif table_name == 'crm_stage':
        columns_13 = '"id","name","sequence","requirements","team_id","fold","create_uid","create_date","write_uid","write_date"'
        cur12.execute( "SELECT %s FROM %s ;" % (columns_13, table_name) )
    elif table_name == 'mail_activity':
        columns_13 = '"id","res_model_id","res_model","res_id","res_name","activity_type_id","summary","note","date_deadline","automated","user_id","recommended_activity_type_id","previous_activity_type_id","create_uid","create_date","write_uid","write_date","calendar_event_id"'
        cur12.execute( "SELECT %s FROM %s ;" % (columns_13, table_name) )
    elif table_name == 'payment_acquirer':
        columns_13 = columns_13.replace('combination_indices,','')
        columns_13 = columns_13.replace('can_image_variant_1024_be_zoomed,','')
    elif table_name == 'account_journal':
        columns_13 = '"id","name","code","active","type","default_credit_account_id","default_debit_account_id","sequence_id","refund_sequence_id","sequence","currency_id","company_id","refund_sequence","at_least_one_inbound","at_least_one_outbound","profit_account_id","loss_account_id","bank_account_id","bank_statements_source","alias_id","show_on_dashboard","color","create_uid","create_date","write_uid","write_date","account_online_journal_id","bank_statement_creation"'
        cur12.execute( "SELECT %s FROM %s ;" % (columns_13, table_name) )
        columns_13 += ',invoice_reference_type,invoice_reference_model'
        
    elif table_name == 'crm_team':
        columns_13 = columns_13.replace('sequence,','')
        cur12.execute( "SELECT %s FROM %s ;" % (columns_13, table_name) )
    

    elif table_name == 'res_partner':
        columns_12 = columns_12.replace('supplier,', '')
        columns_12 = columns_12.replace('style', '')
        columns_12 = columns_12.replace('barcode,', '')
        columns_12 = columns_12.replace('delivery_instructions,', '')
        columns_12 = columns_12.replace('customer,', '')
        columns_12 = columns_12[:-1]
        cur12.execute( "SELECT %s FROM %s ;" % (columns_12, table_name) )
    elif table == 'mail_message_res_partner_needaction_rel':
        columns_12 = '"id","mail_message_id","res_partner_id","is_read","mail_id","failure_type","failure_reason"'
        cur12.execute( "SELECT %s FROM %s ;" % (columns_12, table_name) )



        
    for data in cur12.fetchall():
        cont = 0
        lista_columnas = ''
        for columns in data:
            if type(columns) is int or type(columns) is float or str(type(columns)) == "<class 'decimal.Decimal'>":
                lista_columnas += str(columns) + ','           
            elif type(columns) is datetime.datetime or type(columns) is datetime.date:
                lista_columnas += "'" + str(columns) +"',"
            elif type(columns) is str:
                if len(columns) > 0:
                    lista_columnas += "'" + columns.replace("'","''") + "',"
                else:
                    lista_columnas += "null,"
            elif type(columns) is bool:

                lista_columnas += str(columns) + ','
            else:
                try: 
                    if len(columns) > 1:
                        lista_columnas += str(columns) + ','
                    else:
                        lista_columnas += "null,"

                except:
                    lista_columnas += "null,"
            
            cont += 1
                
        lista_columnas = lista_columnas[:-1]
        cur_columns = con13.cursor()
        if table_name in ['iap_account', 'ir_attachment', 'mail_compose_message']:
            cur_columns.execute("""INSERT INTO %s VALUES(%s);""" % (table_name,lista_columnas))
        elif table_name in ['product_product', 'product_template','sale_order_option']:
            cur_columns.execute("""INSERT INTO %s (%s) VALUES(%s);""" % (table_name,columns_13,lista_columnas))
        elif table_name == 'product_pricelist_item':
            cur_columns.execute("""INSERT INTO %s (%s) VALUES(%s);""" % (table_name,columns_13,lista_columnas))
        elif table_name in ['res_users', 'calendar_event','res_partner','mail_channel_partner', 'mail_message_res_partner_needaction_rel']:
            cur_columns.execute("""INSERT INTO %s (%s) VALUES(%s);""" % (table_name,columns_12,lista_columnas))
        elif table_name in ['crm_stage','mail_activity','payment_acquirer', 'crm_team']:
            cur_columns.execute("""INSERT INTO %s (%s) VALUES(%s);""" % (table_name,columns_13,lista_columnas))
        elif table_name in ['account_journal']:
            lista_columnas += ",'invoice','odoo'"
            cur_columns.execute("""INSERT INTO %s (%s) VALUES(%s);""" % (table_name,columns_13,lista_columnas))

        
        
        
        con13.commit()




def migrate_table(con12, con13, table_name):    
    if table_name not in tablas_distintas:
        cur12 = con12.cursor()
        cur12.execute( "SELECT column_name, data_type FROM information_schema.columns where table_name = '%s'" % table_name )

        data_columnas = []
        for data in cur12:
            data_columnas.append(data.data_type)
        


        cur12 = con12.cursor()
        cur12.execute( "SELECT * FROM %s  ;" % table_name )

        for data in cur12.fetchall():
            cont = 0
            lista_columnas = ''
            for columns in data:
                if type(columns) is int or type(columns) is float:
                    lista_columnas += str(columns) + ','           
                elif type(columns) is datetime.datetime:
                    lista_columnas += "'" + str(columns) +"',"
                                    
                elif type(columns) is str:
                    if len(columns) > 0:
                        lista_columnas += "'" + columns.replace("'","''") + "',"
                        
                    else:
                        lista_columnas += "null,"
                elif type(columns) is bool:
                    lista_columnas += str(columns) + ','
                else:
                    lista_columnas += "null,"
                cont += 1
                    
            lista_columnas = lista_columnas[:-1]
            cur_columns = con13.cursor()
            cur_columns.execute("""INSERT INTO %s VALUES(%s);""" % (table_name,lista_columnas))
            con13.commit()
    else:
        migrate_table_differ(con12, con13, table_name)
        
myConnection = pgdb.connect( host=hostname, user=username, password=password, database=database )
myConnection13 = pgdb.connect( host=hostname, user=username, password=password, database=database13 )

tables = [
    # 'account_bank_statement_cashbox',
    # 'account_bank_statement_closebalance',
    # 'account_invoice_confirm',
    # 'account_online_wizard',
    # 'account_unreconcile',
    # 'base_import_import',
    # 'base_import_mapping',
    # 'base_import_tests_models_char',
    # 'base_import_tests_models_char_noreadonly',
    # 'base_import_tests_models_char_readonly',
    # 'base_import_tests_models_char_states',
    # 'base_import_tests_models_char_stillreadonly',
    # 'base_import_tests_models_m2o_related',
    # 'base_import_tests_models_m2o_required_related',
    # 'base_import_tests_models_o2m',
    # 'base_module_update',
    # 'base_module_upgrade',
    # 'bus_bus',
    # 'change_password_wizard',
    # 'crm_dashboard',
    # 'decimal_precision_test',
    # 'ir_demo',
    # 'ir_translation',
    # 'mail_resend_cancel',
    # 'payment_acquirer_onboarding_wizard',
    # 'payment_icon' ya esta la data en 13,
    # 'portal_wizard',
    # 'res_config',
    # 'res_config_installer',
    # 'res_partner_industry' ya esta la data en 13,
    # 'res_users_log' solo es un log no es necesario,
    # 'sale_payment_acquirer_onboarding_wizard',
    # 'validate_account_move',
    # 'web_editor_converter_test_sub',
    # 'web_tour_tour',
    # 'x_crear_leads', 
    # # la tabla no esta en v13
    # 'account_account_account_tag',
    # 'account_account_tag_account_tax_template_rel' no esta en 13
    # 'account_account_tax_default_rel',
    # 'account_account_template_account_tag',
    # 'account_account_template_account_tag',
    # 'account_account_template_tax_rel',
    # 'account_account_type_rel',
    # 'account_analytic_line_tag_rel',
    # account_analytic_tag_account_invoice_line_rel la tabla no existe en v13 VACIA
    # 'account_analytic_tag_account_invoice_tax_rel' no 13 VACIA
    # 'account_analytic_tag_account_move_line_rel',
    # 'account_analytic_tag_account_reconcile_model_rel', no 13 VACIA
    # 'account_analytic_tag_sale_order_line_rel', no 13 vacia
    # 'account_bank_statement_import',
    # 'account_common_journal_report_account_journal_rel',
    # 'account_common_report_account_journal_rel',
    # 'account_fiscal_position_res_country_state_rel',
    # 'account_fiscal_position_template_res_country_state_rel',
    # 'account_invoice_account_invoice_send_rel', no 13 VACIA
    # 'account_invoice_account_move_line_rel', no 13 VACIA
    # 'account_invoice_account_register_payments_rel', no 13 VACIA
    # 'account_invoice_import_wizard_ir_attachment_rel', no 13 VACIA
    # 'account_invoice_line_tax', no 13 vacia
    # 'account_invoice_payment_rel',
    # 'account_invoice_transaction_rel',
    # 'account_journal_account_print_journal_rel',
    # 'account_journal_account_reconcile_model_rel',
    # 'account_journal_account_reconcile_model_template_rel',    
    # 'account_journal_type_rel',
    # 'account_move_line_account_tax_rel',
    # 'account_reconcile_model_res_partner_category_rel',
    # 'account_reconcile_model_res_partner_rel',
    # 'account_reconcile_model_template_res_partner_category_rel',
    # 'account_reconcile_model_template_res_partner_rel',
    # 'account_tax_account_tag', no 13 REVISAR
    # 'account_tax_filiation_rel', tablas identicas y data igual no necesidad de migrar
    # 'account_tax_sale_advance_payment_inv_rel',
    # 'account_tax_template_filiation_rel',
    # 'base_import_tests_models_char_required',
    # 'base_import_tests_models_complex',
    # 'base_import_tests_models_m2o',
    # 'base_import_tests_models_o2m_child',
    # 'base_import_tests_models_preview',
    # 'base_language_install',
    # 'base_partner_merge_automatic_wizard_res_partner_rel',
    # 'base_update_translations',    
    # 'crm_lead2opportunity_partner_mass_res_users_rel',
    # 'crm_lead_crm_lead2opportunity_partner_mass_rel',
    # 'crm_lead_crm_lead2opportunity_partner_rel',
    # 'crm_lead_lost',
    # 'crm_lead_names', no 13 REVISAR
    # 'digest_tip_res_users_rel',
    # 'email_template_attachment_rel',
    # 'email_template_preview_res_partner_rel',
    # 'iap_account',
    # 'ir_act_server_mail_channel_rel',
    # 'ir_act_server_res_partner_rel',
    # 'ir_model_fields_group_rel',
    # 'ir_module_module_dependency', la hace solo odoo
    # 'ir_module_module_exclusion', 
    # 'ir_ui_menu_group_rel', tabla ya populada
    # 'ir_ui_menu_group_rel', tabla ya populada
    # 'ir_ui_view_group_rel', tabla ya populada
    # 'mail_activity_rel',
    # 'mail_activity_type_mail_template_rel',
    # 'mail_channel_mail_wizard_invite_rel',
    # 'mail_channel_moderator_rel',
    # 'mail_channel_res_groups_rel', tabla ya populada
    
    # 'mail_message_res_partner_needaction_rel_mail_resend_message_rel',
    # 'mail_resend_message',
    # 'mail_message_subtype',
    # 'mail_shortcode',
    # 'portal_share',
    # 'product_attribute_custom_value',
    # 'product_packaging',
    # 'product_price_list',
    # purchase_order no existe en 13 tabla vacia
    # 'report_paperformat', tabla ya populada
    # 'sale_order_template',
    # 'sms_send_sms', no existe en 13 tabla vacia
    # 'stock_picking', no existe en 13 parece ser que odoo lo crea
    # 'uom_category', tabla ya populada
    # 'account_analytic_group', 
    # 'account_common_journal_report',
    # 'account_common_report',
    # 'account_invoice_refund', no existe 13 vacia
    # 'account_invoice_send',
    # 'account_online_link_wizard',
    # 'account_online_provider',
    # 'account_payment_method', tabla ya populada
    # 'account_setup_bank_manual_config',
    # 'base_partner_merge_automatic_wizard',
    # 'decimal_precision', tabla ya populada
    # 'fetchmail_server',
    
    # 'ir_demo_failure',
    # 'ir_model', tabla ya populada
    # 'ir_model_data' tabla ya populada,
    # 'ir_rule', tabla ya populada
    # 'mail_blacklist',
    
    # 'product_attribute',
    # 'product_template_attribute_exclusion',
    # 'res_bank',
    # 'resource_calendar',
    # 'sale_advance_payment_inv',
    # 'sale_product_configurator',
    # 'sale_product_configurator', no 13 vacia
    # 'wizard_ir_model_menu_create',
    # 'account_fiscal_position',
    # 'account_payment_term', tabla ya populada
    # 'account_print_journal',
    # 'base_language_import',
    # 'calendar_alarm',
    
    # 'change_password_user',
    # 'ir_actions', tabla populada
    # 'ir_logging',
    # 'ir_mail_server',
    # 'ir_model_access', tabla ya populada
    # 'ir_module_module', tabla ya populada
    # 'ir_property',
    # 'product_attribute_value',
    # 'product_price_history',no 13 irrelevante
    # 'product_pricelist', migrar a mano
    # 'product_template_attribute_line',
    # 'product_template_attribute_value',
    # 'res_currency', ya populada
    # 'resource_test',
    # 'sales_markting_cost' no 13 revisar data
    # 'account_analytic_account',
    # 'account_cash_rounding',
    # 'account_fiscal_position_tax_template',
    # 'account_fiscal_position_template',
    # 'account_fiscal_year',
    # 'account_payment_term_line', ya populada
    # 'crm_lead2opportunity_partner',
    # 'crm_lead2opportunity_partner_mass',
    # 'digest_digest', tabla ya populada
    # 'ir_cron', ya esta populada
    # 'ir_default',
    # 'ir_model_relation', ya esta populada,
    # 'ir_sequence_date_range', ya esta populada
    # 'ir_server_object_lines',
    # 'ir_ui_view_custom',
    # 'mail_activity_type',  ya populada
    # 'mail_moderation',
    # 'payment_token',
    # 'portal_wizard_user',
    # 'res_country', ya populada
    # 'res_country_state', ya populada
    # 'res_currency_rate',
    # 'resource_calendar_leaves', 
    # 'account_analytic_distribution',
    # 'account_fiscal_position_account_template',
    # 'account_fiscal_position_tax',
    # 'barcode_rule', ya populada
    # 'ir_act_client', ya populada
    # 'ir_act_url', ya populada
    # 'ir_ui_view', ya populada
    # 'account_account_template', ya populada
    # 'account_fiscal_position_account',
    # 'account_partial_reconcile',
    # 'email_template_preview',
    # 'ir_model_constraint', ya populada
    # 'mail_alias',
    # 'mail_template', ya populada
    # 'res_partner_bank',ir_act_report_xmlopulada
    # 'account_account', ya populada
    # 'account_bank_statement',
    # 'product_pricelist_item',
    # 'res_config_settings',
    # 'resource_resource',
    # 'sale_order_template_line',
    # 'sale_order_template_option',
    # 'snailmail_letter',
    # 'uom_uom', ya populada
    # 'ir_act_window', ya populada
    # 'product_supplierinfo',
    # 'res_lang', ya populada
    # 'res_users',
    # 'ir_exports',
    # 'ir_demo_failure_wizard',
    # 'bus_presence',
    # 'crm_lost_reason',
    # 'digest_digest_res_users_rel',
    # 'digest_tip',
    # 'res_groups',
    # 'ir_exports_line',
    # 'product_category',
    # 'res_partner_category',
    # 'crm_stage',
    # 'calendar_event', depende de opportunity_id
    # 'ir_filters',
    # 'account_bank_statement_line', 
    # 'ir_model_fields', ya populada
    # 'mail_activity',
    # 'payment_acquirer', tabla ya populada
    # 'product_template',
    # 'product_product',
    # 'account_register_payments',
    # 'res_partner',
    # 'calendar_contacts',
    # 'mail_message',
    # 'mail_mail',
    # 'mail_mail_res_partner_rel',
    # 'ir_attachment',
    # 'mail_channel',
    # 'mail_channel_partner',
    # 'mail_message_res_partner_needaction_rel',
    # 'mail_resend_partner',
    # 'mail_compose_message',
    # 'mail_compose_message_res_partner_rel',
    # 'mail_compose_message_ir_attachments_rel',
    # 'mail_followers',
    # 'mail_followers_mail_message_subtype_rel',
    # 'mail_message_mail_channel_rel',
    # 'tax_adjustments_wizard',
    # 'account_move',
    # 'account_invoice_line',
    # 'account_invoice_line',
    # 'account_tax_template', tabla ya populada
    # 'account_reconcile_model_template',
    # 'account_tax',
    # 'payment_transaction',
    # 'ir_act_server', ya populada
    # 'account_analytic_line',
    # 'account_payment',
    # 'ir_sequence',
    # 'account_journal',
    # 'account_journal_inbound_payment_method_rel',
    # 'account_journal_outbound_payment_method_rel',
    # 'crm_team',
    # 'mail_tracking_value',
    # 'account_reconcile_model',
    



    # 'sale_order_line', depende de sale_order
    # 'ir_act_window_view', Pendiente depende de view    
    # 'sale_order_option', Pendiente saleorder
    # 'calendar_alarm_calendar_event_rel', PENDIENTE depende de calendar_event 
    # 'calendar_event_res_partner_rel', PENDIENTE depende de res_partner y calendar event
    
    # 'account_tax_sale_order_line_rel', PENDIENTE depende de saleorder line
    # 'ir_config_parameter', Pendiente revisar
    # 'crm_lead_tag_rel' PENDIENTE depende de lead y tag,
    # 'ir_act_window_group_rel', PENDIENTE revisar a que hace referencia 
    # 'mail_message_res_partner_rel' Pendiente,
    # 'mail_message_res_partner_starred_rel' Pendiente,
    # 'report_layout', Pendiente revisar view_id






]



for table in tables:
    migrate_table(myConnection, myConnection13, table)

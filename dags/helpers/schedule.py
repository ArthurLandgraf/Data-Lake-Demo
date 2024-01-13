# slots para tabelas mais leves, o codigo no final desse script aloca
# todas tabelas aleatoriamente entre 1 e 6
slot_1 = '5 3 * * 0-6'
slot_2 = '10 3 * * 0-6'
slot_3 = '15 3 * * 0-6'
slot_4 = '20 3 * * 0-6'
slot_5 = '25 3 * * 0-6'
slot_6 = '30 3 * * 0-6'

# slots para tabelas mais pesadas, depois de rodar o codigo no final
# e necessario alterar as tabelas no dict abaixo para slots 7+
# considerar o tamanho da tabela
slot_7 = '40 3 * * 0-6'
slot_8 = '50 3 * * 0-6'
slot_9 = '0 4 * * 0-6'
slot_10 = '10 4 * * 0-6'
slot_11 = '20 4 * * 0-6'
slot_12 = '30 4 * * 0-6'
slot_13 = '40 4 * * 0-6'
slot_14 = '50 4 * * 0-6'
slot_15 = '0 5 * * 0-6'
slot_16 = '10 5 * * 0-6'
slot_17 = '20 5 * * 0-6'
slot_18 = '30 5 * * 0-6'
slot_19 = '40 5 * * 0-6'
slot_20 = '50 5 * * 0-6'
slot_21 = '0 6 * * 0-6' # horario final, dw roda aqui, entao tudo tem que estar dentro antes
slot_22 = '20 3 * * 0-6'
slot_23 = '0 3 * * 0-6'
slot_24 = '40 2 * * 0-6'
slot_25 = '20 2 * * 0-6'

# slots para nrt (near real time)
slot_99 = '0 5-23 * * 0-6' # toda hora fechada entre 5-23h
slot_98 = '*/10 2-23 * * *' # a cada 10 minutos exceto da meia noite as 2h
slot_97 = '5 * * * *' # toda hora no minuto 5
slot_96 = '5 5-23 * * 0-6' # toda hora + 5 min entre 5-23h
slot_95 = '*/13 2-23 * * *' # a cada 13 minutos exceto da meia noite as 2h
slot_94 = '*/2 2-23 * * *' # a cada 2 minutos exceto da meia noite as 2h

# formatacao para o nome da key referente a tabela em questao:
# key_schedule= banco_origem + "_" + schema_origem + "_" + nome_tabela

schedule = {
    # dw
    'demopg_demoschema_demotable': slot_25,
    }


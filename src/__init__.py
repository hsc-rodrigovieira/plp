import logging
import pandas as pd
import config
import secret
from ETL.longaPermanencia import LongaPermanencia

# Configura o logging para gravar logs em um arquivo com formato específico
logging.basicConfig(level=logging.INFO, filename=config.path_log, encoding='utf-8', format="%(asctime)s - %(levelname)s - %(message)s")

if __name__ == '__main__':
    
    # Inicia as classes
    protocolo = LongaPermanencia()

    try:
        query, delete, insert, update, alter = protocolo.get_sql()
        registros, colunas = protocolo.db_dql(
           script=query,
           db_host=secret.db_host,
           db_username=secret.db_user,
           db_password=secret.db_pass
        )
        df = pd.DataFrame(data=registros[0], columns=colunas[0])
        aux = ''
        for i in range(len(df)):
            aux+=f'{df.loc[i,'TP_OPERACAO']}: {df.loc[i,'QTD']}. '
        logging.info(aux)

        if df.loc[df['TP_OPERACAO']=='NA','QTD'].unique() == df['QTD'].sum():
            logging.info("Sem alterações para realizar. Encerrando.")
            pass
        else:
            if df.loc[df['TP_OPERACAO']=='D','QTD'].size > 0:
                try:
                    qtd_registros = protocolo.db_dml(
                        operacao='D',
                        script=delete,
                        db_host=secret.db_host,
                        db_username=secret.db_user,
                        db_password=secret.db_pass                    
                    )
                    logging.info(f'Exclusões: {df.loc[df['TP_OPERACAO']=='D','QTD'].unique()}, Registros: {qtd_registros}')
                except:
                    logging.error(f'Falha na exclusão de {df.loc[df['TP_OPERACAO']=='D','QTD'].unique()} pacientes.')                    

            if df.loc[df['TP_OPERACAO']=='I','QTD'].size > 0:
                try:
                    qtd_registros = protocolo.db_dml(
                        operacao='I',
                        script=insert,
                        db_host=secret.db_host,
                        db_username=secret.db_user,
                        db_password=secret.db_pass                    
                    )
                    logging.info(f'Inclusões: {df.loc[df['TP_OPERACAO']=='I','QTD'].unique()}, Registros: {qtd_registros}')
                except:
                    logging.error(f'Falha na inclusão de {df.loc[df['TP_OPERACAO']=='I','QTD'].unique()} pacientes.')                    

            if df.loc[df['TP_OPERACAO']=='U','QTD'].size > 0:
                try:
                    qtd_registros = protocolo.db_dml(
                        operacao='U',
                        script=update,
                        db_host=secret.db_host,
                        db_username=secret.db_user,
                        db_password=secret.db_pass                    
                    )
                    logging.info(f'Atualizações: {df.loc[df['TP_OPERACAO']=='U','QTD'].unique()}, Registros: {qtd_registros}')
                except:
                    logging.error(f'Falha na atualização de {df.loc[df['TP_OPERACAO']=='U','QTD'].unique()} pacientes.')

            if df.loc[df['TP_OPERACAO']=='A','QTD'].size > 0:
                try:
                    qtd_registros = protocolo.db_dml(
                        operacao='A',
                        script=alter,
                        db_host=secret.db_host,
                        db_username=secret.db_user,
                        db_password=secret.db_pass                    
                    )
                    logging.info(f'Alterações: {df.loc[df['TP_OPERACAO']=='A','QTD'].unique()}, Registros: {qtd_registros}')
                except:
                    logging.error(f'Falha na alteração de {df.loc[df['TP_OPERACAO']=='A','QTD'].unique()} pacientes.')
            else:
                None    
    except:
        # Registra um erro caso algum arquivo não seja encontrado
        logging.error(f"Não foi possível ler o script.")


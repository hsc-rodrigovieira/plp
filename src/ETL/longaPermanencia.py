# Importa bibliotecas
import logging
import oracledb
import config

# Configura o logging
logging.basicConfig(level=logging.INFO, filename=config.path_log, encoding='utf-8', format="%(asctime)s - %(levelname)s - %(message)s")

class LongaPermanencia(object):
    """
    Classe para aplicação do Protocolo de Longa Permanência no MVSoul/PEP.

    Atributos:
        Nenhum
    """    

    def __init__ ( self ):
        """
        Construtor da classe.

        Não retorna nenhum valor.
        """
        # Inicializa o cliente Oracle
        oracledb.init_oracle_client() 
        self.dict_dias = {
            "dias_risco": config.dias_risco_longa_permanencia,
            "dias_protocolo": config.dias_longa_permanencia
        }
        return None

    def get_sql ( self ) -> tuple[str,list,str,list,list]:
        """
        Busca o script SQL.

        Args:
            None

        Returns:
            tuple[str/list]: Tupla contendo os scripts extraídos do arquivo de texto
        """            
        try: 
            with open(config.path_script, "r", encoding='utf-8') as script_sql:
                sqlFile = script_sql.read()                 
            consulta, remove, insere, atualiza, altera = sqlFile.split('/')
            remove = remove.split(';')
            atualiza = atualiza.split(';')
            altera = altera.split(';')
            return consulta, remove, insere, atualiza, altera
        except FileNotFoundError as e:
            logging.error(f"Não foi possível ler o arquivo de script. {e}")
        return None

    def db_dql( self, script=str, db_host=str, db_username=str, db_password=str ) -> tuple[list,list]:
        """
        Consulta dados do banco de dados de acordo com o script SQL fornecido.

        Args:
            script (str): Script SQL para extração de dados.
            db_host (str): Host do banco de dados.
            db_username (str): Usuário do banco de dados.
            db_password (str): Senha do banco de dados.
        Returns:
            tuple[list]: Tupla contendo os dados extraídos (linhas, colunas)
        """
        rows = []
        descr = []
        columns = []         
        # Conecta ao banco de dados
        with oracledb.connect( user=db_username, password=db_password, dsn=db_host ) as connection:
            try:
                # Cria um cursor para executar os comandos SQL
                with connection.cursor() as cursor:
                    cursor.execute(script,self.dict_dias)
                    rows.append(cursor.fetchall())
                    descr.append(cursor.description)
                    columns.append([x[0] for x in descr[0]])
            except ValueError as e:
                logging.error(f"Erro na extração dos dados do banco. {e}")
            except ConnectionError as e:
                logging.error(f"Falha na conexao com o banco de dados. {e}")
        # Retorta dados se sucesso
        return rows, columns

    def db_dml( self, operacao:str, script, db_host:str, db_username:str, db_password:str ) -> str:
        """
        Realiza alterações no banco de dados de acordo com o script SQL fornecido.

        Args:
            operacao (str): Operação a ser realizada.
            script: Script (ou lista com scripts) SQL para manipulação de dados.
            db_host (str): Host do banco de dados.
            db_username (str): Usuário do banco de dados.
            db_password (str): Senha do banco de dados.

        Returns:
            str: Número de registros alterados no banco de dados.
        """            
        # Conecta ao banco de dados
        with oracledb.connect(user=db_username, password=db_password, dsn=db_host) as connection:
            try:
                # Cria um cursor para executar os comandos SQL
                cursor = connection.cursor()
                match operacao:
                    case 'D':
                        for s in script:
                            cursor.execute(s)
                    case 'I':
                        cursor.execute(script,self.dict_dias)
                    case 'U':
                        for s in script:
                            cursor.execute(s,dias_protocolo=config.dias_longa_permanencia)
                    case 'A':
                        for s in script:
                            cursor.execute(s,self.dict_dias)
                    case _:
                        logging.error(f"Tipo de script não reconhecido ou erro nas variáveis.")
                        pass
                inserts = cursor.rowcount
                connection.commit()
                cursor.close()
            except ValueError as e:
                logging.error(f"Erro manipulação dos dados do banco. {e}")
            except ConnectionError as e:
                logging.error(f"Falha na conexao com o banco de dados. {e}")
        # Retorta dados se sucesso
        return inserts
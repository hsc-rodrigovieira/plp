# Protocolo Longa Permanência

Script desenvolvido para automatizar a alteração de status dos pacientes no protocolo de longa permanência.

## Tecnologias Utilizadas
- Python
- SQL Oracle

## Funcionalidades
- Identificar o tempo de internação de todos os pacientes internados.
- Atribuir o respectivo status para os pacientes com tempo de internação maior ou igual a 15 dias.
- Remover o status dos pacientes com alta.

## Estrutura do Projeto
```plaintext
PLP/
│-- src/                               # Código-fonte principal do projeto
│   │-- data/                          # Diretório de dados
│   │-- ETL/                           # Módulos do pipeline ETL
│   │   │-- longaPermanencia.py        # Pipeline principal de ETL
│   │-- config.py                      # Arquivo de configuração do projeto
│   │-- secret.py                      # Arquivo de configuração do acesso ao banco de dados
│   │-- PLP.log                        # Logs de execução do sistema
│   │-- requirements.txt               # Dependências do projeto
│   │-- __init__.py                    # Arquivo de execução da aplicação
│-- .gitignore                         # Arquivos ignorados pelo Git
│-- README.md                          # Documentação do projeto
```

## Instalação e Uso

### Pré-requisitos

Antes de rodar a aplicação, certifique-se de ter instalado Python 3.12

### Passos para Rodar o Projeto

1. Clone este repositório:
   ```bash
   git clone https://github.com/hsc-rodrigovieira/plp.git
   ```

2. Crie um ambiente virtual:
   ```bash
   python -m venv venv
   source venv/bin/activate   # No Windows use: venv\Scripts\activate
   ```

3. Acesse o diretório do projeto:
   ```bash
   cd PLP/scr
   ```

4. Instale as dependências do projeto:
    ```bash
    pip install -r requirements.txt
    ```

5. Edite o arquivo secret.py, adicionando as credenciais de acesso ao banco de dados:
    ```python
    db_user = ""
    db_host = ""
    db_pass = ""
    ```    

5. Crie um arquivo .bat :
    ```bash
    @ECHO OFF
    cd C:/DIRETORIO/RAIZ/DO/PROJETO/.../
    C:/DIRETORIO/DO/AMBIENTE/.../python.exe src/__init__.py
    ```

6. Agende uma tarefa no Task Scheduler do Windows para execução do arquivo .bat diariamente às 00:15.
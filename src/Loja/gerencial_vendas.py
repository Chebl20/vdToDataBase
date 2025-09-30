import requests
import pandas as pd
import base64
from datetime import datetime, timedelta
import json
import io
import time
import logging
import os
from typing import Optional, Dict, Any, List
from dotenv import load_dotenv
import pytest
import time
import json
from selenium import webdriver
from selenium.webdriver.common.action_chains import ActionChains
from selenium.webdriver.support import expected_conditions
from selenium.webdriver.support.wait import WebDriverWait
from selenium.common.exceptions import TimeoutException,NoSuchElementException, StaleElementReferenceException
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.common.desired_capabilities import DesiredCapabilities
from selenium.webdriver.chrome.service import Service 
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from datetime import datetime, timedelta
import pandas as pd
import glob
import os
import numpy as np
from urllib.parse import quote
from sqlalchemy import create_engine, text
from sqlalchemy.types import String
from sqlalchemy.types import Integer
from sqlalchemy.types import Float
from sqlalchemy.types import DateTime
from selenium.webdriver.chrome.options import Options
from selenium.common.exceptions import WebDriverException
import chromedriver_autoinstaller
from dotenv import load_dotenv
import logging
import io
from pandas.api.types import is_scalar
import psycopg2.extras
from seleniumbase import Driver
from psycopg2.extras import execute_values


# Carregando variáveis de ambiente do arquivo .env
load_dotenv()

# Configuração do logger
def setup_logger():
    # Cria o diretório de logs se não existir
    log_dir = "logs"
    if not os.path.exists(log_dir):
        os.makedirs(log_dir)
    
    # Nome do arquivo de log com timestamp
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    log_file = os.path.join(log_dir, f"execucao_{timestamp}.log")
    
    # Configuração do formato do log
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s [%(levelname)s] %(message)s',
        handlers=[
            logging.FileHandler(log_file, encoding='utf-8'),
            logging.StreamHandler()  # Para mostrar logs no console também
        ]
    )
    
    return logging.getLogger(__name__)

# Inicializa o logger
logger = setup_logger()


class BoticarioAuth:
    """Classe responsável pela autenticação na API do Boticário"""
    
    def __init__(self, base_url: str, username: str, password: str):
        self.base_url = base_url
        self.username = username
        self.password = password
        self.access_token = None
        self.refresh_token = None
    
    def authenticate(self) -> bool:
        """
        Realiza a autenticação e armazena os tokens
        
        Returns:
            bool: True se autenticação foi bem-sucedida, False caso contrário
        """
        url_auth = f"{self.base_url}/auth"
        payload = {
            "username": self.username,
            "password": self.password
        }
        
        try:
            response = requests.post(url_auth, json=payload, timeout=10)
            response.raise_for_status()
            
            data = response.json()
            self.access_token = data.get("accessToken")
            self.refresh_token = data.get("refreshToken")
            
            logger.info("Autenticação realizada com sucesso")
            logger.debug(f"Access Token: {self.access_token}")
            
            return True
            
        except requests.exceptions.RequestException as e:
            logger.error(f"Erro na autenticação: {e}")
            return False
    
    def get_headers(self) -> Dict[str, str]:
        """
        Retorna os headers necessários para as requisições
        
        Returns:
            Dict[str, str]: Headers com authorization
        """
        return {
            "Host": "cp11143.retaguarda.grupoboticario.com.br",
            "Content-Type": "application/json",
            "Authorization": self.access_token,
            "Accept": "*/*"
        }


class ReportDataProcessor:
    """Classe responsável por processar dados dos relatórios"""
    
    @staticmethod
    def decode_base64_to_csv(base64_data: str) -> Optional[str]:
        """
        Decodifica dados Base64 para CSV
        
        Args:
            base64_data (str): Dados em Base64
            
        Returns:
            Optional[str]: Dados CSV decodificados ou None se houver erro
        """
        try:
            # Remove BOM se presente
            if base64_data.startswith('77u/'):
                base64_data = base64_data[4:]
            
            decoded_bytes = base64.b64decode(base64_data)
            decoded_text = decoded_bytes.decode('utf-8')
            logger.debug("Base64 decodificado com sucesso")
            return decoded_text
            
        except Exception as e:
            logger.error(f"Erro ao decodificar Base64: {e}")
            return None
    
    @staticmethod
    def csv_to_dataframe(csv_content: str) -> Optional[pd.DataFrame]:
        """
        Converte string CSV para DataFrame pandas
        
        Args:
            csv_content (str): Conteúdo CSV
            
        Returns:
            Optional[pd.DataFrame]: DataFrame ou None se houver erro
        """
        if not csv_content:
            print("❌ Nenhum conteúdo CSV para processar")
            return None
        
        try:
            df = pd.read_csv(io.StringIO(csv_content), delimiter=';')
            logger.info(f"Total de registros processados: {len(df)}")
            logger.info(f"Colunas encontradas: {list(df.columns)}")
            return df
            
        except Exception as e:
            logger.error(f"Erro ao processar CSV: {e}")
            return None


class BoticarioReportDownloader:
    """Classe principal para download de relatórios do Boticário"""
    
    def __init__(self, auth: BoticarioAuth):
        self.auth = auth
        self.base_url = auth.base_url
        self.processor = ReportDataProcessor()
    
    def _make_report_request(self, start_date: str, end_date: str, 
                           list_by: str = "CONSULTOR", 
                           break_by: str = "DATA") -> Optional[str]:
        """
        Faz uma requisição individual para o relatório
        
        Args:
            start_date (str): Data inicial (YYYY-MM-DD)
            end_date (str): Data final (YYYY-MM-DD)
            list_by (str): Campo para listar por
            break_by (str): Campo para quebrar por
            
        Returns:
            Optional[str]: Dados em Base64 ou None se houver erro
        """
        url = f"{self.base_url}/v1/relatorios/gerencial-vendas/gera"
        
        payload = {
            "ativaMetas": True,
            "ativaPersonalizacao": True,
            "calculoMetasComplementares": "GMV",
            "campoOrdenado": "listarPor",
            "dataFinal": end_date,
            "dataInicial": start_date,
            "diasCom": "VENDAS",
            "exibicao": "CSV",
            "horaFinal": "23:59",
            "horaInicial": "00:00",
            "indicadorReceita": "GMV",
            "listarPor": list_by,
            "ordenacao": "asc",
            "personalizacaoId": None,
            "quebrarPor": break_by,
            "sistema": "TODOS",
            "visualizacaoAnoAnterior": "ANO_VAREJO"
        }
        
        try:
            print(f"📥 Baixando dados de {start_date} até {end_date}...")
            response = requests.post(url, headers=self.auth.get_headers(), 
                                   json=payload, timeout=60)
            
            if response.status_code == 200:
                return response.text
            else:
                print(f"❌ Erro {response.status_code}: {response.text[:200]}")
                return None
                
        except Exception as e:
            print(f"❌ Erro na requisição: {e}")
            return None
    
    def _download_by_months(self, start_date: datetime, end_date: datetime, 
                           list_by: str) -> Optional[str]:
        """
        Baixa dados dividindo por meses
        
        Args:
            start_date (datetime): Data de início
            end_date (datetime): Data de fim
            list_by (str): Campo para listar por
            
        Returns:
            Optional[str]: Dados CSV concatenados ou None se houver erro
        """
        all_data = []
        current = start_date
        
        while current <= end_date:
            # Primeiro dia do mês
            month_start = current.replace(day=1)
            
            # Último dia do mês
            if current.month == 12:
                month_end = current.replace(year=current.year + 1, month=1, day=1) - timedelta(days=1)
            else:
                month_end = current.replace(month=current.month + 1, day=1) - timedelta(days=1)
            
            # Ajustar para não passar da data final
            month_end = min(month_end, end_date)
            
            if month_start <= month_end:
                month_data = self._make_report_request(
                    month_start.strftime("%Y-%m-%d"),
                    month_end.strftime("%Y-%m-%d"),
                    list_by
                )
                
                if month_data:
                    csv_data = self.processor.decode_base64_to_csv(month_data)
                    if csv_data:
                        # Remove header if not the first month
                        if all_data:
                            lines = csv_data.split('\n')
                            if len(lines) > 1:
                                csv_data = '\n'.join(lines[1:])
                        
                        all_data.append(csv_data)
                        print(f"✅ Mês {month_start.strftime('%Y-%m')}: {csv_data.count('\n')} linhas")
                
                time.sleep(1)  # Rate limiting
            
            # Next month
            if current.month == 12:
                current = current.replace(year=current.year + 1, month=1, day=1)
            else:
                current = current.replace(month=current.month + 1, day=1)
        
        return '\n'.join(all_data) if all_data else None
    
    def _download_by_weeks(self, start_date: datetime, end_date: datetime, 
                          list_by: str) -> Optional[str]:
        """
        Baixa dados dividindo por semanas
        
        Args:
            start_date (datetime): Data de início
            end_date (datetime): Data de fim
            list_by (str): Campo para listar por
            
        Returns:
            Optional[str]: Dados CSV concatenados ou None se houver erro
        """
        all_data = []
        current = start_date
        
        while current <= end_date:
            week_end = current + timedelta(days=6)
            week_end = min(week_end, end_date)
            
            week_data = self._make_report_request(
                current.strftime("%Y-%m-%d"),
                week_end.strftime("%Y-%m-%d"),
                list_by
            )
            
            if week_data:
                csv_data = self.processor.decode_base64_to_csv(week_data)
                if csv_data:
                    # Remove header if not the first week
                    if all_data:
                        lines = csv_data.split('\n')
                        if len(lines) > 1:
                            csv_data = '\n'.join(lines[1:])
                    
                    all_data.append(csv_data)
                    print(f"✅ Semana {current.strftime('%Y-%m-%d')}: {csv_data.count('\n')} linhas")
            
            time.sleep(0.5)  # Rate limiting
            current = week_end + timedelta(days=1)
        
        return '\n'.join(all_data) if all_data else None
    
    def download_complete_report(self, start_date: str, end_date: str, 
                               list_by: str = "CONSULTOR") -> Optional[pd.DataFrame]:
        """
        Baixa relatório completo para um período
        
        Args:
            start_date (str): Data inicial (YYYY-MM-DD)
            end_date (str): Data final (YYYY-MM-DD)
            list_by (str): Campo para agrupamento
            
        Returns:
            Optional[pd.DataFrame]: DataFrame com os dados ou None se houver erro
        """
        print(f"\n📊 BAIXANDO RELATÓRIO COMPLETO")
        print(f"Período: {start_date} até {end_date}")
        print(f"Agrupamento: {list_by}")
        print("=" * 60)
        
        # Tentativa 1: Baixar todo o período de uma vez
        complete_data = self._make_report_request(start_date, end_date, list_by)
        
        if complete_data:
            csv_content = self.processor.decode_base64_to_csv(complete_data)
            
            if csv_content:
                lines = csv_content.count('\n')
                print(f"📈 Total de linhas baixadas: {lines}")
                
                if lines > 1:  # More than just header
                    return self.processor.csv_to_dataframe(csv_content)
                else:
                    print("⚠️  Poucos dados retornados, tentando estratégia alternativa...")
        
        # Tentativa 2: Baixar por intervalos menores
        print("🔄 Tentando baixar por intervalos menores...")
        
        start_dt = datetime.strptime(start_date, "%Y-%m-%d")
        end_dt = datetime.strptime(end_date, "%Y-%m-%d")
        days_diff = (end_dt - start_dt).days
        
        if days_diff > 30:
            print(f"📅 Período muito grande ({days_diff} dias), dividindo em meses...")
            csv_content = self._download_by_months(start_dt, end_dt, list_by)
        else:
            print(f"📅 Baixando por semanas...")
            csv_content = self._download_by_weeks(start_dt, end_dt, list_by)
        
        if csv_content:
            return self.processor.csv_to_dataframe(csv_content)
        else:
            print("❌ Falha ao baixar dados com estratégias alternativas")
            return None


class TratarDados:
    """Classe para tratamento de dados"""
    
    @staticmethod
    def clean_dataframe(df: pd.DataFrame) -> pd.DataFrame:
        """
        Realiza limpeza básica no DataFrame
        
        Args:
            df (pd.DataFrame): DataFrame original
            
        Returns:
            pd.DataFrame: DataFrame limpo
        """
        if df is None or df.empty:
            logger.warning("DataFrame vazio ou None, nada a limpar")
            return df
            
        # Debug: Mostra todas as colunas disponíveis
        print("\nColunas disponíveis no DataFrame:")
        print(df.columns.tolist())
            
        # Debug: Mostra os valores originais das colunas antes da conversão
        colunas_debug = ['GMV-Qtd de boletos', 'GMV-Itens por boleto']
        for col in colunas_debug:
            if col in df.columns:
                print(f"\nValores originais da coluna {col}:")
                print(df[col].head())
        
        # Exemplo de limpeza: remover colunas desnecessárias, tratar NaNs, etc.
        df = df.dropna(how='all')  # Remove linhas completamente vazias
        df = df.dropna(axis=1, how='all')  # Remove colunas completamente vazias
        
        # Tratamento da coluna 'Listar Por Consultor'
        if 'Listar Por Consultor' in df.columns:
            # Divide a coluna em ID e Nome do consultor
            
            df[['id_consultor', 'nome_consultor']] = df['Listar Por Consultor'].str.split(' - ', n=1, expand=True)
            
            # Converte ID para inteiro e trata possíveis erros
            df['id_consultor'] = pd.to_numeric(df['id_consultor'], errors='coerce').fillna(0).astype(int)
            
            # Remove a coluna original após o split
            df = df.drop('Listar Por Consultor', axis=1)
            
            print("✅ Coluna 'Listar Por Consultor' processada com sucesso")
        
        
        mapeamento_colunas = {
                    'GMV-GMV': 'gmv',
                    'Quebrar Por Data': 'data',
                    'GMV-Boleto médio': 'boleto_medio',
                    'GMV-Qtd de boletos': 'qtd_boletos',
                    'GMV-Itens por boleto': 'itens_boleto',
                    'Valores de vendas-Receita líquida': 'receita_liquida',
                    'Valores de vendas-Receita líquida (-) trocas': 'receita_liquida_trocas',
                    'Valores de vendas-B1': 'vendas_b1',
                    'Fidelidade-Qtd de boletos': 'fidelidade_qtd_boletos',
                    'Fidelidade-Penetração Boletos': 'fidelidade_penetracao_boletos',  # Corrigido maiúscula em "Boletos"
                    'Descontos-Total de descontos': 'total_descontos',
                    'Trocas-Trocas': 'trocas',
                    'Trocas-Qtd de trocas': 'qtd_trocas',
                    'Cartão presente-Recarga': 'presente_recarga',
                    'Quantitativo-B1': 'qtd_b1',
                }
        
        colunas_existentes = {k: v for k, v in mapeamento_colunas.items() if k in df.columns}
        df = df.rename(columns=colunas_existentes)
        
        # Tratamento de colunas de data
        colunas_data = ['data']
        for col in colunas_data:
            if col in df.columns:
                df[col] = pd.to_datetime(df[col], format='%d/%m/%Y', errors='coerce')
                print(f"✅ Coluna de data '{col}' convertida com sucesso")

        # Tratamento de colunas numéricas/monetárias
        colunas_monetarias = [
            'gmv', 'boleto_medio', 'receita_liquida', 
            'receita_liquida_trocas', 'vendas_b1', 
            'total_descontos', 'trocas', 'presente_recarga'
        ]
        
        # Trata valores monetários (converte string para float)
        for col in colunas_monetarias:
            if col in df.columns:
                # Remove possíveis símbolos monetários e pontos de milhar, substitui vírgula por ponto
                df[col] = df[col].astype(str).str.replace('R$', '', regex=False)
                df[col] = df[col].str.replace('.', '', regex=False)
                df[col] = df[col].str.replace(',', '.', regex=False)
                df[col] = pd.to_numeric(df[col], errors='coerce')
                print(f"✅ Coluna monetária '{col}' convertida com sucesso")

        # Tratamento de colunas numéricas inteiras
        colunas_inteiras = [
            'qtd_boletos', 'itens_boleto', 'fidelidade_qtd_boletos',
            'qtd_trocas', 'qtd_b1'
        ]
        
        # Converte para números inteiros
        for col in colunas_inteiras:
            if col in df.columns:
                # Primeiro limpa a coluna de possível formatação
                df[col] = df[col].astype(str).str.replace('.', '', regex=False)  # Remove pontos de milhar
                df[col] = df[col].str.replace(',', '.', regex=False)  # Troca vírgula por ponto
                
                # Converte para float primeiro para não perder decimais
                df[col] = pd.to_numeric(df[col], errors='coerce')
                
                # Agora converte para inteiro, arredondando para cima
                df[col] = df[col].fillna(0).round().astype(int)
                
                print(f"✅ Coluna inteira '{col}' convertida com sucesso")
                # Imprime alguns valores para verificação
                print(f"Amostra de valores da coluna {col}:")
                print(df[col].head())

        # Tratamento de porcentagens
        colunas_porcentagem = ['fidelidade_penetracao_boletos']
        for col in colunas_porcentagem:
            if col in df.columns:
                # Debug: mostra valores originais
                print(f"\nValores originais da coluna {col} antes da conversão:")
                print(df[col].head())
                
                # Tratamento mais robusto para porcentagens
                df[col] = df[col].astype(str)
                df[col] = df[col].str.replace('%', '', regex=False)  # Remove %
                df[col] = df[col].str.replace('.', '', regex=False)  # Remove pontos de milhar
                df[col] = df[col].str.replace(',', '.', regex=False)  # Converte vírgula para ponto
                
                # Converte para número e divide por 100
                df[col] = pd.to_numeric(df[col], errors='coerce') / 100
                
                print(f"\nValores convertidos da coluna {col}:")
                print(df[col].head())
                print(f"✅ Coluna de porcentagem '{col}' convertida com sucesso")
        
        print("✅ Limpeza de dados concluída")
        return df

class BoticarioReportConfig:
    """Classe para configurações de relatórios"""
    
    REPORT_TYPES = [
        {"list_by": "CONSULTOR", "break_by": "DATA", "name": "por_consultor"},
    ]
    
    @classmethod
    def get_default_config(cls) -> List[Dict[str, str]]:
        """Retorna configuração padrão de relatórios"""
        return cls.REPORT_TYPES
    
  


class Banco():
    def __init__(self):
        print(os.getenv('url_local'))
        time.sleep(2)
        self.engine = create_engine(os.getenv('url_local')) 
        try:
            conn = self.engine.connect()
            result = conn.execute(text("SELECT 1"))
            if result.scalar() == 1:
                print("Conectado ao banco com sucesso!")
                self.criar_tabela()
            else:
                print("Erro ao conectar ao banco")
            conn.close()
        except Exception as e:
            print(f"Erro ao conectar ao banco: {e}")
            raise
        
    def criar_tabela(self):
        try:
            with self.engine.connect() as conn:
                # Criação da tabela (sem índices inline; PostgreSQL não suporta a sintaxe INDEX dentro do CREATE TABLE)
                conn.execute(text("""
                    CREATE TABLE IF NOT EXISTS gerencial_vendas (
                        data DATE NOT NULL,
                        gmv DECIMAL(15,2),
                        boleto_medio DECIMAL(15,2),
                        qtd_boletos INTEGER,
                        itens_boleto INTEGER,
                        receita_liquida DECIMAL(15,2),
                        receita_liquida_trocas DECIMAL(15,2),
                        vendas_b1 DECIMAL(15,2),
                        fidelidade_qtd_boletos INTEGER,
                        fidelidade_penetracao_boletos DECIMAL(5,2),
                        total_descontos DECIMAL(15,2),
                        trocas DECIMAL(15,2),
                        qtd_trocas INTEGER,
                        presente_recarga DECIMAL(15,2),
                        qtd_b1 INTEGER,
                        id_consultor INTEGER NOT NULL,
                        nome_consultor VARCHAR(255),
                        PRIMARY KEY (id_consultor, data)
                    )
                """))

                # Criação dos índices em comandos separados (PostgreSQL)
                conn.execute(text("""
                    CREATE INDEX IF NOT EXISTS idx_gerencial_vendas_data
                    ON gerencial_vendas (data)
                """))
                conn.execute(text("""
                    CREATE INDEX IF NOT EXISTS idx_gerencial_vendas_id_consultor
                    ON gerencial_vendas (id_consultor)
                """))
                conn.execute(text("""
                    CREATE INDEX IF NOT EXISTS idx_gerencial_vendas_consultor_data
                    ON gerencial_vendas (id_consultor, data)
                """))

                conn.commit()
                logger.info("Tabela e índices criados/verificados com sucesso!")
                
        except Exception as e:
            logger.error(f"Erro ao criar tabela: {e}")
        

    def inserir_gerencial_vendas(self, vendas):
        try:
            if hasattr(vendas, 'to_dict'):
                vendas = vendas.to_dict('records')

            # Lista com as colunas exatas e na ordem correta
            colunas = [
                "data", "gmv", "boleto_medio", "qtd_boletos", "itens_boleto", 
                "receita_liquida", "receita_liquida_trocas", "vendas_b1", 
                "fidelidade_qtd_boletos", "fidelidade_penetracao_boletos", 
                "total_descontos", "trocas", "qtd_trocas", "presente_recarga", 
                "qtd_b1", "id_consultor", "nome_consultor"
            ]

            # Filtrar e ordenar os dados conforme as colunas definidas
            values = [tuple(p.get(col, None) for col in colunas) for p in vendas]

            insert_query = f"""
                INSERT INTO gerencial_vendas ({', '.join(colunas)})
                VALUES ({', '.join(['%s'] * len(colunas))})
                ON CONFLICT (id_consultor, data)
                DO UPDATE SET
                    {', '.join([f"{col} = EXCLUDED.{col}" for col in colunas if col not in ['id_consultor', 'data']])}
                """

            conn = self.engine.raw_connection()
            cursor = conn.cursor()
            try:
                psycopg2.extras.execute_batch(cursor, insert_query, values)
                conn.commit()
                logger.info("Inserção/atualização de vendas gerenciais concluída com sucesso")
            except Exception as e:
                conn.rollback()
                logger.error(f"Erro ao inserir/atualizar vendas gerenciais: {str(e)}")
                raise
        except Exception as e:
            logger.error(f"Erro ao inserir/atualizar vendas gerenciais: {str(e)}")
            raise
    
    def fechar(self):
        """Método para fechar o navegador"""
        try:
            if self.engine:
                self.engine.dispose()
        except Exception as e:
            print(f"Erro ao fechar o navegador: {e}")
    
    def consulta(self):
        try:
            with self.engine.connect() as conn:
                result = conn.execute(text("SELECT DISTINCT ciclocaptacao FROM pedidos"))
                return result
        except Exception as e:
            print(f"Erro ao conectar ao banco: {e}")
            raise

   


def main():
    """Função principal"""
    # Configurações
    BASE_URL = "https://cp11143.retaguarda.grupoboticario.com.br/api"
    USERNAME = "Mariliad02"
    PASSWORD = "BotiWeb!"
    
    # Período desejado (automático: de 3 dias atrás até hoje)
    today = datetime.now().date()
    START_DATE = (today - timedelta(days=3)).strftime("%Y-%m-%d")
    END_DATE = today.strftime("%Y-%m-%d")
    
    # Inicializar autenticação
    auth = BoticarioAuth(BASE_URL, USERNAME, PASSWORD)
    
    if not auth.authenticate():
        print("❌ Falha na autenticação. Encerrando...")
        return
    
    # Inicializar downloader
    downloader = BoticarioReportDownloader(auth)
    
    # Baixar relatórios
    report_configs = BoticarioReportConfig.get_default_config()
    
    for config in report_configs:
        print(f"\n{'='*80}")
        print(f"📊 INICIANDO DOWNLOAD: {config['name']}")
        print(f"{'='*80}")
        
        df = downloader.download_complete_report(
            START_DATE, 
            END_DATE, 
            config['list_by']
        )
        
        if df is not None:
            print(f"\n📋 Primeiras linhas do relatório {config['name']}:")
            print(df.head())
            
            # Aqui você pode adicionar processamento adicional dos dados
            # Por exemplo: salvar em arquivo, enviar para banco de dados, etc.
            
        else:
            print(f"❌ Falha ao baixar relatório {config['name']}")
            
        tratar  = TratarDados()
        df = tratar.clean_dataframe(df)
        
        banco = Banco()
        banco.criar_tabela()
        banco.inserir_gerencial_vendas(df)
        banco.fechar()
        
        print(f"\n⏳ Aguardando 3 segundos...")
        time.sleep(3)


if __name__ == "__main__":
    main()
import requests
import pandas as pd
import base64
from datetime import datetime, timedelta
import json
import io
import time
from typing import Optional, Dict, Any, List


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
            
            print(f"✅ Autenticação realizada com sucesso")
            print(f"Access Token: {self.access_token}")
            
            return True
            
        except requests.exceptions.RequestException as e:
            print(f"❌ Erro na autenticação: {e}")
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
            return decoded_bytes.decode('utf-8')
            
        except Exception as e:
            print(f"❌ Erro ao decodificar Base64: {e}")
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
            print(f"📈 Total de registros: {len(df)}")
            print(f"🏷️ Colunas: {list(df.columns)}")
            return df
            
        except Exception as e:
            print(f"❌ Erro ao processar CSV: {e}")
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
            print("⚠️  DataFrame vazio ou None, nada a limpar")
            return df
        
        # Exemplo de limpeza: remover colunas desnecessárias, tratar NaNs, etc.
        df = df.dropna(how='all')  # Remove linhas completamente vazias
        df = df.dropna(axis=1, how='all')  # Remove colunas completamente vazias
        
        
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


def main():
    """Função principal"""
    # Configurações
    BASE_URL = "https://cp11143.retaguarda.grupoboticario.com.br/api"
    USERNAME = "Mariliad02"
    PASSWORD = "BotiWeb!"
    
    # Período desejado
    START_DATE = "2025-09-10"
    END_DATE = "2025-09-11"
    
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
        df.to_excel(f"relatorio_{config['name']}.xlsx", index=False)
        
        print(f"\n⏳ Aguardando 3 segundos...")
        time.sleep(3)


if __name__ == "__main__":
    main()
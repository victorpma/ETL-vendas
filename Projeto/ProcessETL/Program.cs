using Oracle.ManagedDataAccess.Client;
using System;
using System.Data;

namespace ProcessETL
{
    class Program
    {
        private static string _conexaoBancoOperacional = "Data Source=Vendas;User Id=system;Password=2141178589;";
        private static string _conexaoBancoDataWarehouse = "Data Source=//localhost/XE;User Id=system;Password=2141178589";

        #region Stagging Area
        static DataTable tabelaClientesOperacional;
        static DataTable tabelaPedidosOperacional;
        static DataTable tabelaItensDePedidosOperacional;
        static DataTable tabelaProdutosOperacional;
        static DataTable tabelaFornecedoresOperacional;
        static DataTable tabelaNotasFiscaisOperacional;
        static DataTable tabelaItensDeNotaOperacional;
        static DataTable tabelaParcelasOperacional;

        static DataTable dimensaoTempo;        
        static DataTable dimensaoProdutos;
        static DataTable dimensaoTiposVendas;
        static DataTable fatosVendas;
        #endregion

        static void Main(string[] args)
        {
            try
            {
                RealizarExtracao();
                RealizarTransformacao();
                RealizarCarga();
            }
            catch (Exception exception)
            {

                Console.WriteLine("---------- ERRO! ----------");
                Console.WriteLine("Ocorreu um erro: {0}", exception.Message);
            }

            Console.ReadLine();
        }

        #region RealizarExtracao
        private static void RealizarExtracao()
        {
            Console.WriteLine("---------- EXTRAÇÃO BANCO OPERACIONAL ----------");

            tabelaClientesOperacional = ExtrairTabelaArtistas();
            tabelaPedidosOperacional = ExtrairTabelaGravadoras();
            tabelaItensDePedidosOperacional = ExtrairTabelaTiposSocios();
            tabelaProdutosOperacional = ExtrairTabelaSocios();
            tabelaFornecedoresOperacional = ExtrairTabelaItensLocacoes();
            tabelaNotasFiscaisOperacional = ExtrairTabelaTitulos();
            tabelaItensDeNotaOperacional = ExtrairTabelaCopias();
            tabelaParcelasOperacional = ExtrairTabelaCopias();
        }
        #endregion

        #region RealizarTransformacao
        private static void RealizarTransformacao()
        {
            Console.WriteLine("---------- TRANSFORMAÇÃO TABELA -> DIMENSÃO ----------");

            TransformarTabelaArtistasEmDimensao();
            TransformarTabelaGravadorasEmDimensao();
            TransformarTabelaSociosEmDimensao();
            TransformarTabelaTitulosEmDimensao();
            TransformarTabelaLocacoesEmFatos();
        }
        #endregion

        #region [PRIVATE] ExtrairTabelaClientes
        private static DataTable ExtrairTabelaClientes()
        {
            DataTable tabelaClientes = new DataTable();

            using (var conexao = new OracleConnection(_conexaoBancoOperacional))
            {
                conexao.Open();

                Console.WriteLine("Extraindo tabela CLIENTES...");

                OracleCommand commandSQL = conexao.CreateCommand();

                commandSQL.CommandText = "SELECT * FROM CLIENTES";

                commandSQL.CommandType = CommandType.Text;

                OracleDataReader dr = commandSQL.ExecuteReader();

                tabelaClientes.Load(dr);

                Console.WriteLine("Tabela ARTISTAS extraída com sucesso - {0} resultado(s).\n", tabelaClientes.Rows.Count);
                return tabelaClientes;
            }
        }

        #region [PRIVATE] ExtrairTabelaPedidos
        private static DataTable ExtrairTabelaPedidos()
        {
            DataTable tabelaPedidos = new DataTable();

            using (var conexao = new OracleConnection(_conexaoBancoOperacional))
            {
                conexao.Open();

                Console.WriteLine("Extraindo tabela PEDIDOS...");

                OracleCommand commandSQL = conexao.CreateCommand();

                commandSQL.CommandText = "SELECT * FROM PEDIDOS";

                commandSQL.CommandType = CommandType.Text;

                OracleDataReader dr = commandSQL.ExecuteReader();

                tabelaPedidos.Load(dr);
            }

            Console.WriteLine("Tabela PEIDODS extraída com sucesso - {0} resultado(s).\n", tabelaPedidos.Rows.Count);
            return tabelaPedidos;
        }
        #endregion

        #region [PRIVATE] ExtrairTabelaItensDePedidos
        private static DataTable ExtrairTabelaItensDePedidos()
        {
            DataTable tabelaItensDePedidos = new DataTable();

            using (var conexao = new OracleConnection(_conexaoBancoOperacional))
            {
                conexao.Open();

                Console.WriteLine("Extraindo tabela ITENS_DE_PEDIDOS...");

                OracleCommand commandSQL = conexao.CreateCommand();

                commandSQL.CommandText = "SELECT * FROM GRAVADORAS";

                commandSQL.CommandType = CommandType.Text;

                OracleDataReader dr = commandSQL.ExecuteReader();

                tabelaItensDePedidos.Load(dr);
            }

            Console.WriteLine("Tabela ITENS_DE_PEDIDO extraída com sucesso - {0} resultado(s).\n", tabelaItensDePedidos.Rows.Count);
            return tabelaItensDePedidos;
        }
        #endregion

        #region [PRIVATE] ExtrairTabelaProdutos
        private static DataTable ExtrairTabelaProdutos()
        {
            DataTable tabelaProdutos = new DataTable();

            using (var conexao = new OracleConnection(_conexaoBancoOperacional))
            {
                conexao.Open();

                Console.WriteLine("Extraindo tabela PRODUTOS...");

                OracleCommand commandSQL = conexao.CreateCommand();

                commandSQL.CommandText = "SELECT * FROM PRODUTOS";

                commandSQL.CommandType = CommandType.Text;

                OracleDataReader dr = commandSQL.ExecuteReader();

                tabelaProdutos.Load(dr);
            }

            Console.WriteLine("Tabela PRODUTOS extraída com sucesso - {0} resultado(s).\n", tabelaProdutos.Rows.Count);
            return tabelaProdutos;
        }
        #endregion



    }
}


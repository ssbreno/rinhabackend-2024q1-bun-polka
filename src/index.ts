import polka from 'polka';
import { Pool } from "pg";
import { z } from 'zod';

const app = polka();

const pool = new Pool({
    connectionString: process.env.DB_HOSTNAME ?? 'postgres://admin:123@db:5432/rinha',
  });

const transactionSchema = z.object({
    valor: z.number().positive(),
    tipo: z.enum(['c', 'd']),
    descricao: z.string().min(1).max(10),
});

const idSchema = z.object({
    id: z.string().regex(/^\d+$/, 'ID must be an integer').transform(Number),
  });

const selectCustomers = (id: number) => `SELECT * FROM clientes WHERE id = ${id}`;

const createTransaction = (id: number, novoSaldo: number, valor: number, tipo: string, descricao: string) => `
  WITH insere_transacao AS (
    INSERT INTO transacoes (id_cliente, valor, tipo, descricao) VALUES (${id}, ${valor}, '${tipo}', '${descricao}')
    RETURNING id
  )
  UPDATE clientes SET saldo = ${novoSaldo} WHERE id = ${id};
`;

const selectTransactions = (id: number) => `SELECT *
FROM
    transacoes t
WHERE
    t.id_cliente = ${id}
ORDER BY
    t.realizada_em DESC
LIMIT 10`;

  
  const getClient = async (id: number) => {
    const client = await pool.connect();
    try {
      const { rows } = await client.query(selectCustomers(id));
      return rows[0];
    } finally {
      client.release();
    }
  };
  
  const performTransaction = async (id: number, valor: number, tipo: string, descricao: string) => {
    const client = await pool.connect();
    try {
      await client.query('BEGIN');
      const cliente = await getClient(id);
      if (!cliente) throw new Error('Customer not found');
  
      const transactionAmount = tipo === 'c' ? valor : -valor;
      const novoSaldo = cliente.balance + transactionAmount;
  
      if (novoSaldo < -cliente.limit) return { code: 422, data: null }
  
      await client.query(createTransaction(id, novoSaldo, valor, tipo, descricao));
      await client.query('COMMIT');
      return { saldo: novoSaldo, limite: cliente.limit };
    } catch (e) {
      await client.query('ROLLBACK');
      throw new Error('Customer not found');
    } finally {
      client.release();
    }
  };

  const getExtrato = async (customerId : number) => {
    const client = await pool.connect();
    try {
      const getCustomer = await getClient(customerId);
      if (!getCustomer) {
        throw new Error('Customer not found');
      }

      const { rows: transactionsRows } = await client.query(selectTransactions(customerId));

       const ultimasTransacoes = transactionsRows.map((tx) => ({
        valor: tx.valor,
        tipo: tx.tipo,
        descricao: tx.descricao,
        realizada_em: tx.realizada_em.toISOString(),
      }));
    
      const extrato = {
        saldo: {
          total: getCustomer.saldo,
          data_extrato: new Date().toISOString(),
          limite: getCustomer.limite,
        },
        ultimas_transacoes: ultimasTransacoes,
      };
  
      return extrato;
    } catch (error) {
      throw new Error('Customer not found');
    } finally {
      client.release();
    }
  };
  
  app.post('/clientes/:id/transacoes', async (req, res) => {
    try {
      const validatedId = idSchema.parse({ id: req.params.id });
      const validateParams = transactionSchema.parse(req.body);
      const result = await performTransaction(validatedId.id, validateParams.valor, validateParams.tipo, validateParams.descricao);
      res.end(JSON.stringify(result));
    } catch (e) {
      res.statusCode = 404;
      res.end(`Not Found: ${e}`);
    }
  });
  
  app.get('/clientes/:id/extrato', async (req, res) => {
    try {
      const validatedId = idSchema.parse({ id: req.params.id });
      const extrato = await getExtrato(validatedId.id);
      res.end(JSON.stringify(extrato));
    } catch (e) {
      res.statusCode = 404;
      res.end(`Not Found: ${e}`);
    }
  });

app.listen(8000, () => {
    console.log(`> Polka Running on 8000`);
  });
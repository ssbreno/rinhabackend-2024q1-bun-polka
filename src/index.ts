import polka from 'polka';
import { Pool } from "pg";
import { z } from 'zod';

const app = polka();

const pool = new Pool({
    connectionString: process.env.DB_HOSTNAME ?? 'postgres://admin:123@localhost:5432/rinha',
  });

const transactionSchema = z.object({
    valor: z.number().positive(),
    tipo: z.enum(['c', 'd']),
    descricao: z.string().min(1).max(10),
});

const idSchema = z.object({
    id: z.string().regex(/^\d+$/, 'ID must be an integer').transform(Number),
  });

const selectCustomers = (id: number) => `SELECT nome,limite,saldo FROM customers WHERE id = ${id}`;

const createTransaction = (id: number, novoSaldo: number, valor: number, tipo: string, descricao: string) => `
  WITH insere_transacao AS (
    INSERT INTO transactions (id_cliente, valor, tipo, descricao) VALUES (${id}, ${valor}, '${tipo}', '${descricao}')
    RETURNING id
  )
  UPDATE customers SET saldo = ${novoSaldo} WHERE id = ${id};
`;
  
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
      if (!cliente) return { code: 422, data: null }
  
      const transactionAmount = tipo === 'c' ? valor : -valor;
      const novoSaldo = cliente.balance + transactionAmount;
  
      if (novoSaldo < -cliente.limit) return { code: 422, data: null }
  
      await client.query(createTransaction(id, novoSaldo, valor, tipo, descricao));
      await client.query('COMMIT');
      return { saldo: novoSaldo, limite: cliente.limit };
    } catch (e) {
      await client.query('ROLLBACK');
      throw e;
    } finally {
      client.release();
    }
  };

  const getExtrato = async (customerId : number) => {
    console.log(customerId)
    const client = await pool.connect();
    console.log(client)
    try {
      const getCustomer = await getClient(customerId);
      console.log(getCustomer);
      if (getCustomer.length === 0) {
        throw { status: 404};
      }
      const customer = getCustomer[0];
      const transactionsQuery = `
      SELECT
      t.id_cliente,
      t.valor,
      t.tipo,
      t.descricao,
      t.realizada_em
        FROM
            transacoes t
        WHERE
            t.id_cliente = ${customer.id}
        ORDER BY
            t.realizada_em DESC
        LIMIT 10
      `;
      const { rows: transactionsRows } = await client.query(transactionsQuery, [customerId]);
  
      const ultimasTransacoes = transactionsRows.map((tx) => ({
        valor: tx.valor,
        tipo: tx.tipo,
        descricao: tx.descricao,
        realizada_em: tx.realizada_em.toISOString(),
      }));
  
      const extrato = {
        saldo: {
          total: customer.saldo,
          data_extrato: new Date().toISOString(),
          limite: customer.limite,
        },
        ultimas_transacoes: ultimasTransacoes,
      };
  
      return extrato;
    } catch (error) {
      console.error('Error fetching account statement:', error);
      throw error;
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
      throw e;
    }
  });
  
  app.get('/clientes/:id/extrato', async (req, res) => {
    try {
      const validatedId = idSchema.parse({ id: req.params.id });
      console.log(validatedId.id)
      const extrato = await getExtrato(validatedId.id);
      res.end(JSON.stringify(extrato));
    } catch (e) {
      throw e;
    }
  });

app.get('/', (req, res) => {
    res.end('Hello world!');
});


app.listen(8000, () => {
    console.log(`> Polka Running on 8000`);
  });
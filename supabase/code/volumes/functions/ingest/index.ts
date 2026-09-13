// A porta de ingestão do Zapier para o schema `mkt`.
//
// Fina de propósito: confere o segredo, lê o corpo, chama `mkt.ingest`. Toda a
// deduplicação vive no SQL — uma transação só, testável contra um Postgres
// local, e continua valendo se um dia a entrada deixar de ser o Zapier.
//
// AUTENTICAÇÃO é o header `x-ingest-secret`, comparado em tempo constante. Um
// `===` sai no primeiro byte diferente e vaza o segredo por temporização.
// Sem segredo configurado a porta responde 503 e não toca no banco: não existe
// modo degradado que aceite requisição sem prova.
//
// A `service_role` nunca sai daqui. O Zapier recebe só o `x-ingest-secret`.

import { Client } from 'https://deno.land/x/postgres@v0.19.3/mod.ts'

const INGEST_SECRET = Deno.env.get('INGEST_SECRET') ?? ''
const DB_URL = Deno.env.get('SUPABASE_DB_URL') ?? ''

const KINDS = ['lead', 'zoom', 'live_attendance', 'ad_day', 'community_day']

function json(body: unknown, status: number): Response {
  return new Response(JSON.stringify(body), {
    status,
    headers: { 'Content-Type': 'application/json' },
  })
}

/** Comparação que não desiste no primeiro byte diferente. */
function segredoConfere(recebido: string, esperado: string): boolean {
  const a = new TextEncoder().encode(recebido)
  const b = new TextEncoder().encode(esperado)
  if (a.length !== b.length) return false
  let diff = 0
  for (let i = 0; i < a.length; i++) diff |= a[i] ^ b[i]
  return diff === 0
}

Deno.serve(async (req: Request) => {
  if (req.method !== 'POST') {
    return json({ status: 'erro', error: 'Use POST.' }, 405)
  }
  if (!INGEST_SECRET || !DB_URL) {
    return json({ status: 'erro', error: 'not configured' }, 503)
  }

  const recebido = req.headers.get('x-ingest-secret') ?? ''
  if (!segredoConfere(recebido, INGEST_SECRET)) {
    // Nada sobre o que estava errado: "tamanho errado" já entrega o tamanho.
    return json({ status: 'erro', error: 'forbidden' }, 403)
  }

  const kind = new URL(req.url).searchParams.get('kind') ?? ''
  if (!KINDS.includes(kind)) {
    return json(
      { status: 'erro', error: `kind has to be one of: ${KINDS.join(', ')}` },
      422,
    )
  }

  let payload: unknown
  try {
    payload = await req.json()
  } catch {
    return json({ status: 'erro', error: 'body is not valid JSON' }, 422)
  }

  // O IP de quem chamou, para `mkt.ingest_log`. Um `source_ip` que não é um IP
  // é a marca de carga feita à mão — ver §10 do docs/INGESTAO-ZAPIER.md.
  const ip = (req.headers.get('x-forwarded-for') ?? '').split(',')[0].trim()

  const client = new Client(DB_URL)
  try {
    await client.connect()
    const r = await client.queryObject<{ resultado: Record<string, unknown> }>(
      'select mkt.ingest($1::text, $2::jsonb, $3::text) as resultado',
      [kind, JSON.stringify(payload), ip || 'edge'],
    )
    const out = r.rows[0]?.resultado ?? { status: 'erro', error: 'no result' }
    // `duplicado` é SUCESSO, 200. O Zapier reenvia, e um 4xx aqui viraria um
    // laço de reentregas por aquilo que é a coisa mais comum que existe.
    const code = out?.status === 'erro' ? 422 : 200
    return json(out, code)
  } catch (e) {
    // A falha fica no corpo e no log, não numa exceção que desfaz a linha de
    // `mkt.ingest_log` — é ela que permite reprocessar depois.
    console.error('ingest falhou:', e)
    return json({ status: 'erro', error: String(e).slice(0, 300) }, 500)
  } finally {
    try { await client.end() } catch { /* conexão já caiu */ }
  }
})

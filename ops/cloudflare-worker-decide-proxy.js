export default {
  async fetch(request, env, ctx) {
    const url = new URL(request.url);
    // Proxy only a fixed path to avoid abuse
    const target = 'https://decide.madrid.es/system/api/proposals.csv';
    const ua = request.headers.get('user-agent') || '';
    const headers = {
      'User-Agent': ua,
      'Accept': 'text/csv, text/plain;q=0.9, */*;q=0.1',
      'Referer': 'https://decide.madrid.es/',
      'Accept-Language': request.headers.get('accept-language') || 'es-ES,es;q=0.9,en;q=0.8',
      'Connection': 'keep-alive',
    };
    const resp = await fetch(target, { headers, redirect: 'follow' });
    if (!resp.ok) {
      return new Response('Upstream error', { status: 502 });
    }
    const csv = await resp.arrayBuffer();
    return new Response(csv, {
      headers: {
        'Content-Type': 'text/csv; charset=utf-8',
        'Cache-Control': 'no-store',
      },
    });
  }
}


# G3 · MiniMax Anthropic Messages · laboratorio aislado

## Hallazgo de la fuente oficial de OpenCode v2.0.26

El paquete nativo `@opencode/ai/providers/minimax` exporta `model: messages`. Su ruta predeterminada es `https://api.minimax.io/anthropic/v1/messages`, `minimax-messages` con protocolo Anthropic Messages. También existen rutas opcionales explícitas Chat Completions y Responses. Fuente: https://github.com/anomalyco/opencode/blob/v2.0.26/packages/ai/src/providers/minimax.ts

Esto determina el **default de código**, NO el protocolo efectivamente desplegado en Lunar. `mcp-builder` está vinculado a `minimax/MiniMax-M3`, pero la configuración de proveedor, paquete, hooks y overrides de URL del proceso no es accesible mediante el conector remoto conectado. No asumir default=tráfico real.

## Tests mínimos nuevos

`node --test poc/g3-guard/minimax-messages-lab.test.mjs` comprueba sintéticamente Anthropic Messages, uso de `max_tokens`, caché de lectura/escritura/entrada, streams `message_start` -> `message_delta` -> `message_stop`, herramientas limitadas, presupuesto en vuelo y denegaciones. **No** reejecuta las 34 pruebas anteriores; sus CI son independientes.

No existe en las fuentes oficiales consultadas una API MiniMax verificada de conteo previo para este payload Anthropic Messages. El callback `trustedInputUpperBound` sigue siendo una simulación: sin un límite confiable, HTTP 503 y **cero solicitudes**. Por tanto G3 real **BLOCKED**. El prototipo procesa eventos sintéticos hasta el final; NO retransmite streaming a OpenCode ni soporta todos sus formatos de herramientas, multimodalidad y contexto.

Sin proveedor real, modelos, credenciales, Lunar, despliegue, PR o merge. Para cerrar: obtener prueba del paquete/endpoints efectivos en el proceso de Lunar sin imprimir secretos y una garantía real del límite de input/coste.

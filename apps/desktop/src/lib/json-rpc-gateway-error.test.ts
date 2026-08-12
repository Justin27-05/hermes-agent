import { JsonRpcGatewayClient, JsonRpcGatewayError } from '@hermes/shared'
import { describe, expect, it, vi } from 'vitest'

class FakeSocket {
  static OPEN = 1
  readyState = 0
  private readonly listeners = new Map<string, Set<(event: { data?: string }) => void>>()

  addEventListener(type: string, listener: (event: { data?: string }) => void): void {
    const listeners = this.listeners.get(type) ?? new Set()
    listeners.add(listener)
    this.listeners.set(type, listeners)
  }

  removeEventListener(type: string, listener: (event: { data?: string }) => void): void {
    this.listeners.get(type)?.delete(listener)
  }

  close(): void {
    this.emit('close')
  }

  emit(type: string, event: { data?: string } = {}): void {
    for (const listener of this.listeners.get(type) ?? []) {
      listener(event)
    }
  }

  send = vi.fn()

  open(): void {
    this.readyState = FakeSocket.OPEN
    this.emit('open')
  }
}

describe('JsonRpcGatewayClient RPC errors', () => {
  it('preserves JSON-RPC code and untrusted data behind a typed safe error', async () => {
    const socket = new FakeSocket()
    const client = new JsonRpcGatewayClient({ socketFactory: () => socket as unknown as WebSocket })
    const connecting = client.connect('ws://127.0.0.1:7000/rpc')

    socket.open()
    await connecting

    const pending = client.request('project.command', { project_id: 'project-a' })
    const request = JSON.parse(String(socket.send.mock.calls[0][0]))

    socket.emit('message', {
      data: JSON.stringify({
        error: { code: 4091, data: { current_control_version: 7 }, message: 'sensitive backend detail' },
        id: request.id,
        jsonrpc: '2.0'
      })
    })

    await expect(pending).rejects.toMatchObject({
      code: 4091,
      data: { current_control_version: 7 },
      message: 'Hermes RPC failed'
    })
    await expect(pending).rejects.toBeInstanceOf(JsonRpcGatewayError)
  })
})

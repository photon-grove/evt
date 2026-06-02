import '@testing-library/jest-dom/vitest'

// React Flow measures the DOM via ResizeObserver, which jsdom does not provide.
// A no-op shim keeps smoke renders from throwing.
class ResizeObserverStub {
  observe(): void {}
  unobserve(): void {}
  disconnect(): void {}
}

if (typeof globalThis.ResizeObserver === 'undefined') {
  globalThis.ResizeObserver = ResizeObserverStub as unknown as typeof ResizeObserver
}

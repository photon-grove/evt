import hljs from 'highlight.js/lib/core'
import bash from 'highlight.js/lib/languages/bash'
import go from 'highlight.js/lib/languages/go'
import ini from 'highlight.js/lib/languages/ini'
import json from 'highlight.js/lib/languages/json'
import rust from 'highlight.js/lib/languages/rust'
import sql from 'highlight.js/lib/languages/sql'
import typescript from 'highlight.js/lib/languages/typescript'
import yaml from 'highlight.js/lib/languages/yaml'

// Only the languages the docs use, so the bundle stays small. Fences with any other tag (text,
// mermaid, none) render as plain code.
hljs.registerLanguage('bash', bash)
hljs.registerLanguage('go', go)
hljs.registerLanguage('ini', ini)
hljs.registerLanguage('json', json)
hljs.registerLanguage('rust', rust)
hljs.registerLanguage('sql', sql)
hljs.registerLanguage('typescript', typescript)
hljs.registerLanguage('yaml', yaml)
hljs.registerAliases(['sh', 'shell', 'console'], {languageName: 'bash'})
hljs.registerAliases(['toml'], {languageName: 'ini'})
hljs.registerAliases(['ts', 'tsx'], {languageName: 'typescript'})

// languageOf reads the fence tag marked puts on a <code> element ("language-rust,ignore" -> "rust").
export function languageOf(code: Element): string | undefined {
  const tag = Array.from(code.classList).find((name) => name.startsWith('language-'))

  return tag?.slice('language-'.length).split(/[,\s]/)[0]
}

// highlightCode returns highlighted HTML for a known language, or undefined to leave it plain.
export function highlightCode(code: string, language: string): string | undefined {
  if (!hljs.getLanguage(language)) {
    return undefined
  }

  return hljs.highlight(code, {language, ignoreIllegals: true}).value
}

// highlightBlocks highlights every fenced code block under container in place.
export function highlightBlocks(container: HTMLElement) {
  container.querySelectorAll<HTMLElement>('pre > code').forEach((code) => {
    const language = languageOf(code)
    if (!language || code.dataset.highlighted === 'yes') {
      return
    }

    const html = highlightCode(code.textContent ?? '', language)
    if (html !== undefined) {
      code.innerHTML = html
      code.classList.add('hljs')
      code.dataset.highlighted = 'yes'
    }
  })
}

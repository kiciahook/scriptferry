## Requirements

- VS Code 1.125 or newer.
- Node.js and npm to build from source.
- For the reference listener: A Roblox environment that exposes `game:HttpGet`, `HttpService`, `task.wait` and `loadstring`.

## Install from source

```sh
npm install
npm run compile
npm run package
code --install-extension scriptferry-0.1.0.vsix
```

## Development

```sh
npm run compile
npm test
npm run package
```

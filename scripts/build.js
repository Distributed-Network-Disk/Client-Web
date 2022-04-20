import esbuild from 'esbuild'

esbuild.build({
  entryPoints: ['src/index.ts'],
  outfile: 'dist/index.js',
  bundle: true,
  minify: true,
  target: 'es2020',
  format: 'esm',
})

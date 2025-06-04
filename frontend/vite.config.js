import { defineConfig } from 'vite'
import react from '@vitejs/plugin-react'

export default defineConfig({
  plugins: [react()],
  server: {
    host: true,       // Allow access from network IPs
    strictPort: false,
    cors: true,       // Enable CORS for easier dev
  },
})

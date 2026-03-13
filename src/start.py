import simple_api

def main():
    server = simple_api.HTTPServer((simple_api.HOST, simple_api.PORT), simple_api.ApiHandler)
    print(f"API listening on http://{simple_api.HOST}:{simple_api.PORT}/status")
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        print("\nStopping API server...")
        server.server_close()

if __name__ == "__main__":
    main()
from extract_custinvoicetrans import process_custinvoicetrans
from extract_inventdim import process_inventdim

def main():
    print("Iniciando procesamiento de tablas...")
    process_custinvoicetrans()
    process_inventdim()
    print("Procesamiento completado.")

if __name__ == "__main__":
    main()

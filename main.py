from extract_custinvoicetrans import process_custinvoicetrans
from extract_inventdim import process_inventdim
from extract_custinvoicejour import process_custinvoicejour

def main():
    print("Iniciando procesamiento de tablas...")
    process_custinvoicetrans()
    process_inventdim()
    process_custinvoicejour()
    print("Procesamiento completado.")

if __name__ == "__main__":
    main()

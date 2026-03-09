import sys

def main():
    first = "final-output/answer.txt"
    second = "final-output/merged.txt"

    with open(first, "rb") as f1, open(second, "rb") as f2:
        same = f1.read() == f2.read()
    print(same)
    raise SystemExit(0 if same else 1)

if __name__ == "__main__":
    main()

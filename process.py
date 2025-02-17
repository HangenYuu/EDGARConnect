import re
import os
from bs4 import BeautifulSoup
from concurrent.futures import ThreadPoolExecutor, as_completed
import polars as pl


def parse_sec_filing(file_path: str) -> dict[str, str]:
    """
    Parse SEC 10-K filing and extract key information.

    Args:
        file_path: Path to the SEC filing text file

    Returns:
        Dictionary containing company name, business description and risk factors
    """
    with open(file_path, "r", encoding="utf-8") as f:
        content = f.read()

    result = {"Company Name": "", "Business": "", "Risk Factors": ""}

    company_match = re.search(r"COMPANY CONFORMED NAME:\s*(.+)(?:\r\n|\r|\n)", content)
    if company_match:
        result["Company Name"] = company_match.group(1).strip()

    current_section = None
    current_content = []

    soup = BeautifulSoup(content, "lxml")
    paragraphs = soup.find_all("p")

    for p in paragraphs:
        text = p.get_text(strip=True)

        business_match = re.match(r"Item\s*1\.\s*Business", text)
        risk_match = re.match(r"Item\s*1A\.\s*Risk\s*Factors", text)
        next_section_match = re.match(r"Item\s*(1B|2)\.", text)

        if business_match:
            current_section = "Business"
            current_content = []
        elif risk_match:
            if current_section:
                result[current_section] = " ".join(current_content)
            current_section = "Risk Factors"
            current_content = []
        elif next_section_match:
            if current_section:
                result[current_section] = " ".join(current_content)
            current_section = None
        elif current_section and text:
            current_content.append(text)

    if current_section and current_content:
        result[current_section] = " ".join(current_content)

    return result


def process_files_concurrently(directory: str):
    files = [
        os.path.join(directory, f) for f in os.listdir(directory) if f.endswith(".txt")
    ]
    results = []

    with ThreadPoolExecutor(max_workers=64) as executor:
        futures = {executor.submit(parse_sec_filing, file): file for file in files}
        for future in as_completed(futures):
            try:
                result = future.result()
                results.append(result)
            except Exception as e:
                print(f"Error processing file {futures[future]}: {e}")

    return results


def save_to_parquet(data: list[dict[str, str]], output_file: str):
    df = pl.DataFrame(data)
    df.write_parquet(output_file)


if __name__ == "__main__":
    directory = "/home/jupyter-hangenyuu/EDGARConnect/documents/10-K"
    output_file = "/home/jupyter-hangenyuu/EDGARConnect/sec_filings.parquet"

    results = process_files_concurrently(directory)
    save_to_parquet(results, output_file)

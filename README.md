# Quantamental Screening

Welcome to the Quantamental Screening GitHub repository. This project aims to classify firms based on Benjamin Graham's criteria as detailed in "The Intelligent Investor" by leveraging financial statement data from the SEC (Securities and Exchange Commission) website. The repository includes multiple methods to retrieve and process this data, enabling robust financial analysis.

## Overview

This repository utilizes four different methods to connect to the SEC website to retrieve financial statements (balance sheet, income statement, and statement of cash flows) for selected financial metrics (e.g., Revenues, COGS, Goodwill, Current Assets, CFO, CFI) from 10-Q documents for over 8000 unique tickers. These methods allow for the classification of firms based on the following conditions derived from Benjamin Graham's criteria:

### Defensive Conditions
1. Total revenues >= $2000 million
2. Total current assets >= 2 * total current liabilities
3. Net current assets >= long-term debt
4. EPS growth: last EPS >= 1.33 * previous EPS
5. Current price <= 1.5 * BVPS
6. Current price <= 2/3 * intrinsic value
7. Current ratio between 1.2 and 4
8. Multiplier PB <= 22.5

### Enterprising Conditions
1. Total current assets >= 1.5 * total current liabilities
2. Net current assets >= total debt * 1.1
3. Positive net income
4. Current price < 1.2 * TBVPS

### NCAV Condition
Net current asset value >= current price

## Methods

### 1. Frames
Utilizes customized URLs to get selected facts for all firms. This method is relatively fast but provides more "instantaneous" data rather than quarterly data, making it useful for market signal adjustments but insufficient for classification due to limited data.

### 2. Facts
Adjusts URLs based on CIK to scrape selected facts. Recommended for a balance between accuracy and reduced runtime. This method is less precise but faster than the "concept" method.

### 3. Concept
Creates URLs based on combinations of CIK and fact, generating over 300,000 unique URLs. This method is the most precise and suitable for historical data but requires the longest runtime.

### 4. Selenium
Naive method that scrapes the SEC website by identifying patterns in the URLs of the 10-Q reports and extracting account numbers from the HTML source. This method is detailed and customizable. Initially tested on 20 firms, but more can be added. It's highly detailed but requires customization for additional firms and facts.

## File Structure

### Main Files
- **CompanyScreeningTechniques.ipynb**: Combines all methods into a single workflow.
- **CompanyFrames.ipynb**: Implementation of the "frames" method.
- **CompanyFacts.ipynb**: Implementation of the "facts" method.
- **CompanyConcept.ipynb**: Implementation of the "concept" method.
- **CompanySeleniumMultiple.ipynb**: Implementation of the "selenium" method for multiple firms.
- **CompanySeleniumSingle.ipynb**: Test file for single firm using the "selenium" method.

### Utility Files
- **Quantamental_functions.py**: Contains all functions used in the Jupyter notebooks.

### Data Folders
- **DataRaw**: Contains the Excel file with all selected facts.
- **DataDerived**: Contains all pickle files for quick data loading.
- **DataSummaries**: Contains Excel files with classification results for each method and a comprehensive classification analysis (by method, industry, market cap bucket and P/E ratio bucket).

## How to Use

1. Clone the repository:
    ```bash
    git clone https://github.com/miindisponi99/Quantamental-Screening.git
    ```
2. Navigate to the repository:
    ```bash
    cd Quantamental-Screening
    ```
3. Run the Jupyter notebooks to perform firm classifications using your preferred method.

## Notes

- Single firm classification is possible by searching for the company name and CIK using any of the methods.
- All methods save their output in pickle format for quick reloading and analysis.
- Detailed analysis and classifications are available in the `DataSummaries` folder.

## Contributions

Contributions are welcome! Please feel free to submit issues or pull requests.

## License

This project is licensed under the MIT License.

---

We hope you find this project useful for your financial analysis needs. If you have any questions or feedback, please open an issue in this repository.

Happy Screening!

📄 Production Weekly Extractor — Version 1 Release Notes
Overview

Version 1 of the Industrial Pixel Production Weekly Extractor marks the first fully packaged and production-ready release of the tool. It provides a complete workflow for parsing Production Weekly PDFs, extracting structured information, organizing productions into region buckets, and generating comparison reports with a clean, reliable interface.

This release emphasizes stability, intelligent parsing, polished UI design, and full automation. The tool is delivered as a standalone Windows application (Production_Weekly_Extractor.exe), complete with bundled dependencies and Industrial Pixel branding for seamless deployment.

🔥 Major Features in Version 1
1. Full Standalone Windows Application

Version 1 ships as a complete EXE, requiring no external Python installation.

Fully packaged using PyInstaller

Includes:

Custom application icon

Branded Industrial Pixel letterhead/logo

Bundled dependencies (PyMuPDF, pandas, geonamescache, pycountry, rapidfuzz, Pillow, etc.)

End users can launch the tool instantly without setup.

2. Complete GUI Interface

The graphical interface is designed for clarity, polish, and ease of use.

Core UI Features

Three-tab navigation:

Build

Compare (Old vs New)

Master Compare

Uniformly sized action buttons placed side-by-side

Updated Industrial Pixel color system:

Horizon Blue

Midnight / Slate

Clean white and muted text hierarchy

Scrollbars styled to match IP branding

Prominent centered IP banner in the header

Refined spacing, padding, and layout logic

Quality-of-life Features

Automatic line wrapping for long text fields

Popup detail windows for full cell contents

Shift + Mouse Wheel for horizontal scrolling

Preview window stays contained within a single monitor

Alternating row striping for readability

3. Parsing Engine

The parsing core intelligently extracts structured data from Production Weekly PDFs.

✔ Production Title Detection

Handles:

Quoted titles

Unquoted uppercase titles

Titles with type or network suffixes

Titles containing small date codes

✔ Date Parsing

Supports flexible formats, such as:

March 2 – April 7, 2027

March 2, 2026 - April 7, 2027

Missing-year spans

Automatic year-rollover correction

✔ Location Detection

Accurately identifies and normalizes:

Multi-location productions

City → Region → Country

USA, Canada, Australia, UK, EU, Asia

Fuzzy-matched cities via geonamescache

Common industry hubs (LA, NYC, Vancouver, Montreal, London, Sydney)

Typos auto-corrected (e.g., "ontartio" → Ontario, "newyork" → New York).

✔ Production Office Detection

Cleaner extraction of:

Company names

Addresses

Suite/floor metadata

✔ Production Company Mapping

Detects canonical studios including:

Amazon MGM

Sony Pictures

Warner Bros

Lionsgate

Netflix

Apple

Paramount

Disney

And more

4. Master Compare System

Version 1 includes a comprehensive Master Compare workflow.

✔ Region Buckets

Automatically routes productions into:

United States

West Coast Canada

East Coast Canada

Quebec

Ireland/Hungary

Australia/New Zealand

Europe/Other

Other

✔ Batch Mode (All Regions)

Runs comparisons across every region and produces:

One combined summary file

Individual outputs per region

Global count of productions with date changes

✔ Updated vs Master Detection

Smart comparison logic checks:

Titles

Types

Shooting dates

City / Province / Country

Director/Producer

Production Company

Handles blank weekly fields gracefully to avoid false “updates.”

✔ Existing CSV Preview Button

Allows instant loading of previously generated Master Compare CSVs straight into the UI.

5. Build & Compare Tools
Build Tool

Removes bottom watermark

Extracts text page-by-page

Splits productions cleanly

Generates deterministic FullSchema CSV files

Compare Tool (Old vs New Issue)

Fuzzy matching for renamed productions

Smart detection of:

New productions

Updated productions

Removed productions

Clean summary output

6. Stability and Error Handling

Version 1 includes robust protections to avoid failure:

Fully bundled dependencies

Improved error messages

Handling of empty fields

Prevents oversized preview window

Title duplication eliminated

More stable AKA / W/T parsing

Cleaner distinction between Description and Production Office

Prevents summary overwrites

7. Branding & Presentation

New high-res Industrial Pixel header

Polished application icon

Consistent, branded color scheme

Professional, modern software feel

8. Distribution Package

The final build includes:

Production_Weekly_Extractor.exe
PW_Extractor_Long-01-01.png
PW_Extractor-01-01-01.ico


With all internal libraries included inside the packaged bundle.
Users do not need Python installed.

🎉 Summary

Version 1 of the Production Weekly Extractor is the first fully realized, production-ready release of this tool. It brings together deep parsing logic, a clean and fully branded interface, and automated workflows for building, comparing, and organizing Production Weekly data across regions. This tool is now ready for real-world deployment at Industrial Pixel and serves as the foundation for future enhancements.
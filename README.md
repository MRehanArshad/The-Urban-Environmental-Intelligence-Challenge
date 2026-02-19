# The-Urban-Environmental-Intelligence-Challenge
I will be analyzing the OpenAQ Golbal Air Quality API data for an entire year of 2025

---

# Setup

### Step 1 : Setup Environment

1. Clone the repository:

```code
git clone https://github.com/MRehanArshad/The-Urban-Environmental-Intelligence-Challenge.git
```

2. Setup virtual environment 

```code
python -m venv venv
```

3. Activate virtual environment

```code
venv/Scripts/activate
```

---

### Step 2: 

1. In the Repoistory folder, add a ```.env``` file, and add a key named ```OPENAQ_API_KEY```

```code
OPENAQ_API_KEY=<your-openaq-api-here>
```

# Run Project

If you want to run project and also download the data:

```code
python main.py
```

And, if you want to skip download data step:

```code
python main.py --skip-download
```

---

For any problem

**Contact:** f233098@cfd.nu.edu.pk
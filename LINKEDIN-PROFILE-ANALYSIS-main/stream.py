import streamlit as st
import pandas as pd
import seaborn as sns
import matplotlib.pyplot as plt
import company as cps

company_list = ["Select",'tata consultancy services', 'accenture', 'amazon', 'self-employed', 'microsoft', 'infosys', 'ibm', 'ey', 'freelance', 'cognizant', 'deloitte', 'capgemini', 'google', 'hcl technologies', 'apple', 'pwc', 'oracle', 'self employed', 'us army', 'td'] 
#company = st.text_input("Enter the Company Name")
company = st.selectbox("Select the Company Name",company_list)
if st.button("Submit", key="submit_fill"):
    if company and company!="Select":
        univ,qual,skills = cps.getCompanyData(company)

        st.markdown(f"Top 5 Universities for Employees at {company.capitalize()}")
        fig, ax = plt.subplots(figsize=(10, 6))
        sns.barplot(data=univ, x="count", y="university", palette="viridis", ax=ax)
        ax.set_title(f"Top 5 Universities for Employees at {company.capitalize()}")
        ax.set_xlabel("Count")
        ax.set_ylabel("Universities")
        plt.tight_layout()
        st.pyplot(fig)

        palette_color = sns.color_palette('bright') 
        st.markdown(f"Qualifications at {company.capitalize()}")
        fig, ax = plt.subplots(figsize=(10, 6))
        plt.pie(qual['count'], labels=qual['degree_name'],colors=palette_color, autopct='%1.1f%%', startangle=140)
        ax.set_title(f"Qualifications at {company.capitalize()}")
        plt.tight_layout()
        st.pyplot(fig)

        st.markdown(f"Top 20 Skills for Employees at {company.capitalize()}")
        fig, ax = plt.subplots(figsize=(10, 6))
        sns.barplot(data=skills, x="count", y="skill", palette="viridis", ax=ax)
        ax.set_title(f"Top 20 Skills for Employees at {company.capitalize()}")
        ax.set_xlabel("Count")
        ax.set_ylabel("Skills")
        plt.tight_layout()
        st.pyplot(fig)
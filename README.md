# **Highlands Coffee Survey Analysis: Brand Positioning and Customer Segmentation**
- Author: Nguyen Tran Xuan Truong
- Tool Used: Python and Power BI
- Project Status: Work in progress
    - Dashboard design: Finished
    - Reporting: Work in progress

# **Table of Contents**
# **Background and Overview**
# **Objectives**
The objective of this analysis is to build a comprehensive, data-driven understanding of customer behavior, brand perception, competitive dynamics and loyalty performance, while evaluating how these patterns have evolved over time. By integrating historical data and comparing market trends against 2025 performance, the analysis identifies shifts in customer needs, changes in brand engagement and emerging opportunities or risks. This holistic view enables Highlands Coffee to pinpoint leakage points across the customer journey, assess competitive positioning, anticipate market movements and make informed decisions that strengthen customer engagement and long-term growth.

# **Business Questions**
1. How effectively do Highlands Coffee and its competitors advance customers through the conversion funnel from awareness to loyalty, and how do their awareness and familiarity levels compare across key customer segments?

2. Which stages of the customer journey show the highest customer drop-off and which stages demonstrate strong progression, and what underlying factors explain these patterns in retention and attrition?

3. What core perceptions and brand attributes do customers most strongly associate with Highlands Coffee, and how well do these perceptions reflect the brand’s intended market positioning?

4. How is Highlands Coffee positioned relative to competitors on key perceptual maps comparing Price with Product Quality and Price with Store Atmosphere, and what strategic advantages or gaps does this reveal?

5. Which demographic groups contribute the most to high-value and premium purchases, and which emerging demographics show increasing purchase frequency or spending potential?

6. What factors most strongly influence purchasing decisions across different customer need states and times of day, and how do these drivers vary by segment and context?

7. What is the relationship between customers’ perceptions of the brand and their likelihood to purchase or recommend?

8. Which demographic groups exhibit the highest churn risk and which demonstrate the strongest loyalty potential?

9. How satisfied are customers across different behavioral segments, and how does satisfaction correlate with loyalty?

# **Dataset Background**
The dataset comprises nine tables covering survey respondents’ information, brand image, brand health, and respondents’ need states when purchasing products. A detailed data dictionary can be accessed through the provided [link](https://github.com/Truong2003-tqd/Highlands-Customer-Analysis/tree/11753423d64e7e75a2097c2e93d1f14a4146f266/Data%20Dictionary).

However, the data contained significant integrity issues, including a high rate of missing values, inconsistent labeling, and grammatical errors. To address these problems, Python was used as the primary tool for data detection, cleaning, and preprocessing. The dataset was fully cleaned using pandas and NumPy within Jupyter Notebook.

A detailed report of the data errors and the preprocessing steps can be accessed through the provided [link](https://github.com/Truong2003-tqd/Highlands-Customer-Analysis/tree/11753423d64e7e75a2097c2e93d1f14a4146f266/Preprocessing%20Notebook).

# **Dashboard Description**

## Overview Dashboard

**Description:** This dashboard summarizes the total respondent base and provides a demographic profile across key attributes such as gender, age, occupation, and income level. It also visualizes the geographic distribution of respondents and highlights the composition of major customer segments. 

Customer segments shown here are derived from a K-means clustering model. Detailed methodology and interpretation will be presented in the subsequent sections.

![Overview Dashboard](Image/Screenshot%202025-12-12%20035156.png)

## Customer Behavior Analysis

**Description:** This page analyzes customer activity patterns, covering peak visiting hours, peak days of the week, and the types of activities customers engage in during different time periods. It also highlights the key factors customers prioritize when choosing a coffee shop, offering a clear understanding of what influences store selection. Together, these insights help identify optimal staffing windows, promotional timing, product positioning opportunities, and experience improvements tailored to customer behaviors and preferences.

![Customer Behavior Analysis](Image/Screenshot%202025-12-12%20035207.png)

## Competitive Landscape

**Description:** This dashboard evaluates the competitive landscape of major coffee brands, providing a structured view of how key players compare in store scale, growth momentum, brand familiarity, and perceptual positioning. It highlights each brand’s market footprint, awareness levels across consumers, and their relative strengths on critical perception attributes such as popularity, environment, quality, and multi-purpose usage. The page supports strategic benchmarking by revealing where the brand stands versus competitors and identifying opportunities to differentiate or reinforce market position.

![Competitive Landscape](Image/Screenshot%202025-12-12%20124908.png)

## Customer Insight Analysis

**Description:** This dashboard explains customer perceptions and the value drivers that influence their decisions. It presents how key perception metrics relate to one another and demonstrates which customer segments show stronger awareness of the brand. The page also examines how different levels of familiarity affect the importance customers place on factors such as environment, product quality, popularity and service. The purpose is to help identify what customers value most and where meaningful perception opportunities exist.

![Customer Insight Analysis](Image/Screenshot%202025-12-12%20035234.png)

## Conversion Analysis

**Description:** This dashboard examines how effectively coffee brands move customers through the conversion funnel and identifies where retention strengthens or weakens. It compares funnel performance across major competitors, highlights which customer groups are most at risk of dropping off at each stage and reveals the brand attributes that keep customers engaged once they progress. The purpose is to pinpoint leakage points, understand what motivates continued engagement and guide targeted actions that improve conversion and long term customer retention.

![Conversion Analysis](Image/Screenshot%202025-12-12%20130839.png)

## RFM Analysis

**Description:** This dashboard provides a detailed assessment of customer loyalty through RFM segmentation. It tracks churn, at-risk behavior and the distribution of Bronze, Silver and Gold customers. It visualizes how each RFM group differs in visit frequency and spending and presents how loyalty rates vary across demographic and geographic profiles. The purpose is to identify which segments require retention interventions, which groups show strong value potential and where loyalty patterns differ by city, age, income or occupation. 

RFM segments shown here are derived from a K-means clustering model. Detailed methodology and interpretation will be presented in the subsequent sections.

![RFM Analysis](Image/Screenshot%202025-12-12%20035247.png)

## Need Satisfaction Analysis

**Description:** This dashboard evaluates customer satisfaction across both RFM segments and behavioral segments. It highlights how satisfaction varies by loyalty level and shows how closely satisfaction aligns with churn risk. It also analyzes satisfaction across primary customer needs such as beverages, socializing and business activities. In addition, it identifies which store attributes customers mention most often beyond their core needs, helping to distinguish must-have expectations from value-enhancing qualities. The purpose is to pinpoint satisfaction gaps, understand what drives positive experiences and guide actions that strengthen customer retention and loyalty.

![Need Satisfaction Analysis](Image/Screenshot%202025-12-12%20035302.png)

<!-- Improved compatibility of back to top link: See: https://github.com/othneildrew/Best-README-Template/pull/73 -->
<a id="readme-top"></a>

# cloudtree-mwaa

<!-- TABLE OF CONTENTS -->
<!-- <details>
  <summary>Table of Contents</summary>
  <ol>
    <li>
      <a href="#about-the-project">About The Project</a>
      <ul>
        <li><a href="#built-with">Built With</a></li>
      </ul>
    </li>
    <li>
      <a href="#getting-started">Getting Started</a>
      <ul>
        <li><a href="#prerequisites">Prerequisites</a></li>
        <li><a href="#installation">Installation</a></li>
      </ul>
    </li>
    <li><a href="#usage">Usage</a></li>
    <li><a href="#roadmap">Roadmap</a></li>
    <li><a href="#contributing">Contributing</a></li>
    <li><a href="#license">License</a></li>
    <li><a href="#contact">Contact</a></li>
    <li><a href="#acknowledgments">Acknowledgments</a></li>
  </ol>
</details> -->

## About The Project

- cloudtree 프로젝트 - 도서관 활성화를 위한 통합 데이터 분석 플랫폼 구축 프로젝트 

- 배치 파이프라인 자동화 시스템 - Amazon Managed Workflows for Apache Airflow (MWAA)

<p align="right">(<a href="#readme-top">back to top</a>)</p>


## Built With

[![AWS][AWS-badge]][AWS-url]


[![AWS MWAA][MWAA-badge]][MWAA-url]
[![Amazon CloudWatch][CloudWatch-badge]][CloudWatch-url]

[![AWS Lambda][Lambda-badge]][Lambda-url]
[![Amazon S3][S3-badge]][S3-url]

[![Amazon Redshift][Redshift-badge]][Redshift-url]
[![AWS Glue][Glue-badge]][Glue-url]
[![AWS Athena][Athena-badge]][Athena-url]
[![AWS QuickSight][QuickSight-badge]][QuickSight-url]

[![Python][Python-badge]][Python-url]
[![Slack API][Slack-badge]][Slack-url]

<!-- URLS -->
[AWS-badge]: https://img.shields.io/badge/AWS-%23232F3E?style=for-the-badge&logo=amazonwebservices&logoColor=white
[AWS-url]: https://aws.amazon.com/

[Lambda-badge]: https://img.shields.io/badge/AWS%20Lambda-%23FF9900?style=for-the-badge&logo=awslambda&logoColor=white
[Lambda-url]: https://aws.amazon.com/lambda/

[Python-badge]: https://img.shields.io/badge/Python-%233776AB?style=for-the-badge&logo=python&logoColor=white
[Python-url]: https://www.python.org/

[MWAA-badge]: https://img.shields.io/badge/AWS%20MWAA-%23CD2264?style=for-the-badge&logo=apacheairflow&logoColor=white
[MWAA-url]: https://aws.amazon.com/managed-workflows-for-apache-airflow/

[CloudWatch-badge]: https://img.shields.io/badge/Amazon%20CloudWatch-%23FF4F8B?style=for-the-badge&logo=amazoncloudwatch&logoColor=white
[CloudWatch-url]: https://aws.amazon.com/cloudwatch/

[Slack-badge]: https://img.shields.io/badge/Slack%20API-%234A154B?style=for-the-badge&logo=slack&logoColor=white
[Slack-url]: https://api.slack.com/

[S3-badge]: https://img.shields.io/badge/Amazon%20S3-%23569A31?style=for-the-badge&logo=amazons3&logoColor=white
[S3-url]: https://aws.amazon.com/s3/

[Redshift-badge]: https://img.shields.io/badge/Amazon%20Redshift-%238C4FFF?style=for-the-badge&logo=amazonredshift&logoColor=white
[Redshift-url]: https://aws.amazon.com/redshift/

[Glue-badge]: https://img.shields.io/badge/AWS%20Glue-%238C4FFF?style=for-the-badge
[Glue-url]: https://aws.amazon.com/glue/

[Athena-badge]: https://img.shields.io/badge/AWS%20Athena-%238C4FFF?style=for-the-badge
[Athena-url]: https://aws.amazon.com/athena/

[QuickSight-badge]: https://img.shields.io/badge/AWS%20QuickSight-%238C4FFF?style=for-the-badge
[QuickSight-url]: https://aws.amazon.com/quicksight/

<p align="right">(<a href="#readme-top">back to top</a>)</p>

## Data Architecture

![data-architecture](https://github.com/user-attachments/assets/b3efa9dd-6665-457c-bba9-118a439fe975)

## Batch Workflow

![batch-workflow](https://github.com/user-attachments/assets/bf9ed145-dabc-4b9d-9f14-2eee563aeb3e)

1. API로 Raw 데이터를 가져오는 Lambda 함수 호출

2. Athena 쿼리 실행으로 Raw Table 메타데이터 업데이트

3. Data Transforming 기능을 수행하는 Glue Job 실행

4. Athena 쿼리 실행으로 Transformed Table 메타데이터 업데이트

5. Athena 쿼리 실행으로 기본 분석 실행 후 Aggregated 데이터를 업데이트 

6. Aggreated Bucket에서 데이터를 가져와 심화 분석하는 Redshift 데이터 업데이트

7. Quicksight의 SPICE 데이터 세트를 갱신

# Data Sets

<!-- ![s3-raw-bucket](https://github.com/user-attachments/assets/9b23b4c0-9cc4-4987-8867-4bb91af486a2)

![glue-job](https://github.com/user-attachments/assets/f3682336-4184-4f8c-9fbf-25fa9de33575) -->

<!-- 

- data1 : 인기대출도서

    - 매월 1일 (1개월 주기)

    -  lambda -> athena(raw table msck) -> glue job -> athena(transformed table msck)
    
- data2 : 문화행사

    - 매월 1일 (1개월 주기)

    -  lambda -> athena(raw table msck) -> glue job -> athena(raw table msck) -> glue job -> athena(transformed table msck)

- data3 : 도서관

    - 6월 18일, 12월 18일 (6개월 주기)

    - lambda -> athena(raw table msck) -> glue job -> athena(transformed table msck)

- data4 : 버스정류장

    - 6월 18일, 12월 18일 (6개월 주기)

    - lambda -> athena(raw table msck) -> glue job -> athena(transformed table msck)

- data5 : 지하철역

    - 6월 18일, 12월 18일 (6개월 주기)

    - lambda -> athena(raw table msck) -> glue job -> athena(transformed table msck)
    
- data6 : 도시공원

    - 6월 18일, 12월 18일 (6개월 주기)

    - athena(raw table msck) -> glue job -> athena(transformed table msck)

- DAGs

    - DAG1
        
        - 인기대출도서(data1) 1,2,3,4 
        
        - 문화행사 1,2,3,2,3,4

    - DAG2
        
        - 도서관, 버스정류장, 지하철역 1,2,3,4 
        
        - 공원 2,3,4

    - DAG3: 5,6,7,8

- schedule

-   매월 1일: DAG1 → DAG3

-   6월 18일, 12월 18일: DAG2 → DAG3 -->
import os
import time
import openai


def get_data(prompt):
    openai.api_key = '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'

    response = openai.Completion.create(
        # model="text-davinci-003",
        model="code-davinci-002",
        prompt=prompt,
        temperature=0.7,
        max_tokens=300,
        top_p=1,
        frequency_penalty=0,
        presence_penalty=0
    )
    print(response.choices[0].text)


if __name__ == '__main__':
    appName = os.path.basename(__file__)
    start_time = time.time()
    get_data('用plotly go.Bar写成交量代码')
    end_time = time.time()
    print('{}：执行完毕！！！程序运行时间：耗时{}s，{}分钟'.format(appName,end_time - start_time, (end_time - start_time) / 60))

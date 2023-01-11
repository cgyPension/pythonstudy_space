import os
import time
import openai


def get_data(prompt):
    openai.api_key = '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'

    response = openai.Completion.create(
        model="text-davinci-003",
        # model="text-davinci-002",
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
    get_data('今天吃什么')
    end_time = time.time()
    print('{}：执行完毕！！！程序运行时间：耗时{}s，{}分钟'.format(appName,end_time - start_time, (end_time - start_time) / 60))


def ZDT(self ,data):
    zt ,dt = [] ,[]
    for i ,y in zip(data["plate"] ,data["pct"]):
        if i == "主板":
            if y > 0.096 :
                zt.append(1)
            else:
                zt.append(0)

            if y  < -0.096:
                dt.append(1)
            else:
                dt.append(0)
        if i == "科创板":
            if y > 0.196 :
                zt.append(1)
            else:
                zt.append(0)

            if y  < -0.196:
                dt.append(1)
            else:
                dt.append(0)
        if i == "创业板":
            if y > 0.196 :
                zt.append(1)
            else:
                zt.append(0)

            if y  < -0.196:
                dt.append(1)
            else:
                dt.append(0)
    data['zt'] = zt
    data['dt'] = dt
    # print(data)
    return data

def Plate(self,data):
    data["pct"] = data["close"] / data["preClose"] -1
    code = str(data['code'][:1][0][:3])

    if code < str( 600) :
        if code < str( 300):
            data["plate"] = '主板'
            return data

        data["plate"] = '科创板'
        return data

    if code < str( 688):
        data["plate"] = '主板'
        return data

    data["plate"] = '创业板'
    return data
    # print(data)
    # return data

import React from 'react'
import { View } from 'react-native'
import { Svg, Rect, G } from 'react-native-svg'
import ChartBuilder from '@chartiful/react-native-chart-builder'
import { VerticalBarGraphConfig } from '../../types'

const BarChart = ({
  data,
  labels,
  height,
  width,
  barRadius = 0,
  barWidthPercentage = 0.7,
  barColor,
  style = {},
  baseConfig = {
    data,
    height,
    width
  }
}: VerticalBarGraphConfig) => {
  const chartBuilder = new ChartBuilder({
    data,
    labels,
    height,
    width,
    ...baseConfig
  })

  const baseHeight: number = height - chartBuilder.yAxisLabelHeight
  const barWidth: number = chartBuilder.yLabelSlotWidth * barWidthPercentage
  const slotGap: number = chartBuilder.yLabelSlotWidth - barWidth

  const renderBars = () => {
    return data.map((val: number, i: number) => {
      const barHeight: number = chartBuilder.calcDataPointHeight(val)

      return (
        <Rect
          key={Math.random()}
          x={(i * chartBuilder.yLabelSlotWidth) + (slotGap / 2) + chartBuilder.leftAlignedXAxisLabelWidth}
          y={baseHeight - barHeight}
          rx={barRadius}
          width={barWidth}
          height={barHeight < 0 ? 0 : barHeight}
          fill={barColor}
        />
      )
    })
  }

  return (
    <View style={style} >
      <Svg height={height} width={width}>
        <G>
          {baseConfig.hasXAxisBackgroundLines !== false ? chartBuilder.renderXAxisLines() : null}
        </G>
        <G>
          {baseConfig.hasXAxisLabels !== false ? chartBuilder.renderXAxisLabels() : null}
        </G>
        <G>
          {baseConfig.hasYAxisLabels !== false ? chartBuilder.renderYAxisLabels() : null}
        </G>
        <G>
          {renderBars()}
        </G>
      </Svg>
    </View>
  )
}

export default BarChart

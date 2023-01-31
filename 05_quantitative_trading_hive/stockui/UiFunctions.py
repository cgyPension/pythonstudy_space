import plotly.graph_objects as go
import numpy as np
# Create data
x = np.arange(0, 10, 0.1)
y = np.sin(x)
# Create figure
fig = go.Figure()
# Add trace
fig.add_trace(go.Scatter(
    x=x,
    y=y,
    mode='lines',
    name='sin(x)'
))
# Add shapes
fig.add_shape(
    type='line',
    x0=0,
    y0=0.5,
    x1=10,
    y1=0.5,
    line=dict(
        color="RoyalBlue",
        width=4,
        dash='dot'
    )
)
# Show figure


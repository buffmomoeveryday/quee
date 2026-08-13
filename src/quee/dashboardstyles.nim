const DashboardCss* = """
*,::before,::after{box-sizing:border-box;border-width:0;border-style:solid;border-color:#e5e7eb}
html{line-height:1.5;-webkit-text-size-adjust:100%;font-family:Inter,ui-sans-serif,system-ui,sans-serif}
body{margin:0;background:#f8fafc;color:#0f172a}
a{color:inherit;text-decoration:inherit}button,input,select{font:inherit}
button{cursor:pointer}table{border-collapse:collapse;width:100%}
.min-h-screen{min-height:100vh}.mx-auto{margin-left:auto;margin-right:auto}
.max-w-7xl{max-width:80rem}.p-6{padding:1.5rem}.px-4{padding-left:1rem;padding-right:1rem}
.px-5{padding-left:1.25rem;padding-right:1.25rem}.py-2{padding-top:.5rem;padding-bottom:.5rem}
.py-3{padding-top:.75rem;padding-bottom:.75rem}.py-4{padding-top:1rem;padding-bottom:1rem}
.mb-2{margin-bottom:.5rem}.mb-4{margin-bottom:1rem}.mt-1{margin-top:.25rem}.mt-6{margin-top:1.5rem}
.grid{display:grid}.flex{display:flex}.hidden{display:none}.items-center{align-items:center}
.justify-between{justify-content:space-between}.gap-2{gap:.5rem}.gap-3{gap:.75rem}.gap-4{gap:1rem}
.gap-6{gap:1.5rem}.space-y-2>:not([hidden])~:not([hidden]){margin-top:.5rem}
.overflow-hidden{overflow:hidden}.overflow-x-auto{overflow-x:auto}.rounded{border-radius:.25rem}
.rounded-lg{border-radius:.5rem}.border{border-width:1px}.border-slate-200{border-color:#e2e8f0}
.border-slate-800{border-color:#1e293b}.bg-white{background:#fff}.bg-slate-50{background:#f8fafc}
.bg-slate-100{background:#f1f5f9}.bg-slate-900{background:#0f172a}.bg-emerald-50{background:#ecfdf5}
.bg-amber-50{background:#fffbeb}.bg-rose-50{background:#fff1f2}.bg-indigo-50{background:#eef2ff}
.text-white{color:#fff}.text-slate-500{color:#64748b}.text-slate-600{color:#475569}
.text-slate-700{color:#334155}.text-slate-900{color:#0f172a}.text-emerald-700{color:#047857}
.text-amber-700{color:#b45309}.text-rose-700{color:#be123c}.text-indigo-700{color:#4338ca}
.shadow-sm{box-shadow:0 1px 2px 0 rgb(0 0 0 / .05)}
.text-xs{font-size:.75rem;line-height:1rem}.text-sm{font-size:.875rem;line-height:1.25rem}
.text-lg{font-size:1.125rem;line-height:1.75rem}.text-2xl{font-size:1.5rem;line-height:2rem}
.text-3xl{font-size:1.875rem;line-height:2.25rem}.font-medium{font-weight:500}
.font-semibold{font-weight:600}.font-bold{font-weight:700}.uppercase{text-transform:uppercase}
.tracking-wide{letter-spacing:.025em}.tabular-nums{font-variant-numeric:tabular-nums}
.divide-y>:not([hidden])~:not([hidden]){border-top-width:1px}.divide-slate-100>:not([hidden])~:not([hidden]){border-color:#f1f5f9}
.min-w-full{min-width:100%}.w-full{width:100%}.max-w-xs{max-width:20rem}.truncate{overflow:hidden;text-overflow:ellipsis;white-space:nowrap}
.btn{display:inline-flex;align-items:center;justify-content:center;border-radius:.375rem;padding:.375rem .75rem;font-size:.875rem;font-weight:600}
.btn-primary{background:#0f172a;color:#fff}.btn-muted{background:#f1f5f9;color:#334155}
.btn-danger{background:#fff1f2;color:#be123c}.btn-warning{background:#fffbeb;color:#b45309}
.badge{display:inline-flex;align-items:center;border-radius:9999px;padding:.125rem .5rem;font-size:.75rem;font-weight:600}
.grid-cols-1{grid-template-columns:repeat(1,minmax(0,1fr))}
@media (min-width:768px){.md\:grid-cols-2{grid-template-columns:repeat(2,minmax(0,1fr))}.md\:grid-cols-4{grid-template-columns:repeat(4,minmax(0,1fr))}.md\:flex{display:flex}}
"""

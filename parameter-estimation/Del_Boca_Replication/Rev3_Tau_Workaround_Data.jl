# Compatibility entrypoint for existing launch scripts. The activity
# index is not a time share: draw the full joint population without rejection.
include(joinpath(@__DIR__, "Rev3_Data.jl"))

load_rev3_tau_workaround_data(args...; kwargs...) = load_rev3_data(args...; kwargs...)
